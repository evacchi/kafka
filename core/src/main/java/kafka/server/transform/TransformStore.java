package kafka.server.transform;

import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.MapperFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.fasterxml.jackson.databind.json.JsonMapper;
import kafka.cluster.Partition;
import kafka.server.HostedPartition;
import kafka.server.HostedPartition$;
import kafka.server.KafkaConfig;
import kafka.server.MetadataCache;
import kafka.server.ReplicaManager;
import kafka.transform.TransformTypeConversions;
import org.apache.kafka.common.Node;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.compress.Compression;
import org.apache.kafka.common.message.MetadataResponseData;
import org.apache.kafka.common.message.UpdateMetadataRequestData;
import org.apache.kafka.common.record.MemoryRecords;
import org.apache.kafka.common.record.SimpleRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import scala.Option;

import java.io.FileInputStream;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

public class TransformStore {
    ScheduledExecutorService scheduler = Executors.newSingleThreadScheduledExecutor();

    private static final Logger LOGGER = LoggerFactory.getLogger(TransformStore.class);

    private final ObjectMapper mapper;
    private final ReplicaManager replicaManager;
    private final MetadataCache metadataCache;
    private final KafkaConfig config;

    private final ConcurrentLinkedQueue<Transform> ktransform = new ConcurrentLinkedQueue<>();

    public TransformStore(ReplicaManager replicaManager, MetadataCache metadataCache, KafkaConfig config) {
        this.replicaManager = replicaManager;
        this.metadataCache = metadataCache;
        this.config = config;

        mapper = JsonMapper.builder()
                .disable(SerializationFeature.FAIL_ON_EMPTY_BEANS)
                .disable(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES)
                .enable(MapperFeature.AUTO_DETECT_FIELDS).build();

        // Simulate registration of a plugin.
        scheduler.schedule(this::delayedInit, 3, TimeUnit.SECONDS);
    }

    private void delayedInit() {
        String pluginName = "my-first-plugin";
        String inputTopic = "test";
        String outputTopic = "test-out";
        Map<String, String> config = Map.of();

        try {
            FileInputStream inputStream =
                    new FileInputStream("/Users/evacchi/Devel/dylibso/xtp-demo/plugins/upper/dist/plugin.wasm");
            var manifest = new TransformManifest(
                    inputStream, pluginName, inputTopic, outputTopic, config);
            registerTransform(manifest);
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    public void registerTransform(TransformManifest manifest) {
        String pluginName = manifest.name();
        String inputTopic = manifest.inputTopic();
        String outputTopic = manifest.outputTopic();

        var responses = TransformTypeConversions.topicsMeta(
                Set.of(inputTopic, outputTopic), metadataCache, this.config);
        LOGGER.info("Transform '{}': Metadata = {}", pluginName, responses);
        if (!responses.containsKey(inputTopic)) {
            LOGGER.error("Transform '{}': Could not find input topic '{}'. Transform will not be instantiated.", pluginName, inputTopic);
        } else if (!responses.containsKey(outputTopic)) {
            LOGGER.error("Transform '{}': Could not find output topic '{}'. Transform will not be instantiated.", pluginName, outputTopic);
        } else {
            var resp = responses.get(outputTopic);
            resp.partitions().get(0).leaderId();
            for (MetadataResponseData.MetadataResponsePartition part : resp.partitions()) {
                Option<Node> partitionLeaderEndpoint = metadataCache.getPartitionLeaderEndpoint(outputTopic, part.leaderId(), config.interBrokerListenerName());
                LOGGER.info("Transform '{}': Partition {} Leader Endpoint = '{}'", pluginName, part.partitionIndex(), partitionLeaderEndpoint);
            }
            try {
                Transform transform = Transform.fromManifest(manifest);
                ktransform.add(transform);
                LOGGER.info("Transform '{}': Successfully initialized.", pluginName);
            } catch (IOException e) {
                LOGGER.error("Transform '" + pluginName + "': An error was caught at init time.", e);
                throw new UncheckedIOException(e);
            }
        }
    }

    /**
     * Applies the transforms registered for the given topic to all the given records.
     */
    public Map<MaybeLocalTopicPartition, MemoryRecords> transform(TopicPartition topicPartition, MemoryRecords inputRecords) {
        Map<MaybeLocalTopicPartition, ArrayList<SimpleRecord>> transformedRecords = new HashMap<>();

        for (var in : inputRecords.records()) {
            for (Transform transf : ktransform) {
                TransformManifest manifest = transf.manifest();

                // Apply the given transform if this is registered as its input topic.
                if (manifest.inputTopic().equals(topicPartition.topic())) {
                    var serializableRecord = new kafka.server.transform.Record(
                            topicPartition.topic(),
                            in.key(),
                            in.value(),
                            in.timestamp(),
                            in.headers());

                    // Each transform may result in multiple records
                    var records = transf.transform(serializableRecord, mapper);

                    // For each record, create a SimpleRecord and compute a valid partition.
                    for (var out : records) {
                        var sr = new SimpleRecord(out.timestamp(), out.key(), out.value());
                        MaybeLocalTopicPartition destTopicPartition =
                                findPartition(transf.manifest().outputTopic(), sr);
                        transformedRecords.computeIfAbsent(
                                destTopicPartition,
                                k -> new ArrayList<>()).add(sr);
                    }
                }
            }
        }

        // Once we collect all the results, we can create one MemoryRecords for each TopicPartition
        Map<MaybeLocalTopicPartition, MemoryRecords> resultMap = new HashMap<>();
        for (var pair : transformedRecords.entrySet()) {
            var results = pair.getValue();
            MemoryRecords memoryRecords = MemoryRecords.withRecords(
                    Compression.NONE, results.toArray(SimpleRecord[]::new));
            resultMap.put(pair.getKey(), memoryRecords);
        }

        return resultMap;
    }

    int roundRobinCount = 0;

    /**
     * Prefer a local partition for the output topic. If such a partition is not found
     */
    private MaybeLocalTopicPartition findPartition(String outputTopic, SimpleRecord sr) {
        var responses = TransformTypeConversions.topicsMeta(
                Set.of(outputTopic), metadataCache, this.config);

        var partitions = responses.get(outputTopic).partitions();

        return findNonlocalPartition(outputTopic, partitions);
    }

    private MaybeLocalTopicPartition preferLocalPartition(String outputTopic, List<MetadataResponseData.MetadataResponsePartition> partitions) {
        var localPartition = findLocalPartition(outputTopic, partitions);
        if (localPartition.isPresent()) {
            return new MaybeLocalTopicPartition(
                    Option.empty(),
                    new TopicPartition(
                            outputTopic, localPartition.get().partitionIndex()));
        } else {
            // If no local partition is available, pick the first available nonlocal partition.
            return findNonlocalPartition(
                    outputTopic, partitions);
        }
    }

    private MaybeLocalTopicPartition useNthPartition(
            String outputTopic, int n, List<MetadataResponseData.MetadataResponsePartition> partitions) {
        for (var part : partitions) {
            if (part.partitionIndex() != n) {
                continue;
            }
            Option<Node> partitionLeaderEndpoint = metadataCache.getPartitionLeaderEndpoint(
                    outputTopic, part.leaderId(), config.interBrokerListenerName());
            if (partitionLeaderEndpoint.isDefined()) {
                return new MaybeLocalTopicPartition(
                        partitionLeaderEndpoint,
                        new TopicPartition(outputTopic, part.partitionIndex()));
            }
        }
        throw new UnsupportedOperationException("FIXME: could not find any partition for the given topic. " +
                outputTopic + " This should have been validated earlier.");
    }


    private MaybeLocalTopicPartition findNonlocalPartition(
            String outputTopic, List<MetadataResponseData.MetadataResponsePartition> partitions) {

        int rr = ++roundRobinCount % partitions.size();
        for (var part : partitions) {
            if (part.partitionIndex() != rr) {
                continue;
            }
            Option<Node> partitionLeaderEndpoint = metadataCache.getPartitionLeaderEndpoint(
                    outputTopic, part.leaderId(), config.interBrokerListenerName());
            if (partitionLeaderEndpoint.isDefined()) {
                return new MaybeLocalTopicPartition(
                        partitionLeaderEndpoint,
                        new TopicPartition(outputTopic, part.partitionIndex()));
            } else {
                return preferLocalPartition(outputTopic, partitions);
            }
        }
        throw new UnsupportedOperationException("FIXME: could not find any partition for the given topic. " +
                outputTopic + " This should have been validated earlier.");
    }

    private Optional<MetadataResponseData.MetadataResponsePartition> findLocalPartition(
            String outputTopic, List<MetadataResponseData.MetadataResponsePartition> partitions) {
        return partitions.stream()
                .filter(p -> replicaManager.onlinePartition(new TopicPartition(outputTopic, p.partitionIndex())).isDefined())
                .findFirst();
    }

    public static class MaybeLocalTopicPartition {
        private final Option<Node> maybeNode;
        private final TopicPartition topicPartition;

        public MaybeLocalTopicPartition(Option<Node> maybeNode, TopicPartition topicPartition) {
            this.maybeNode = maybeNode;
            this.topicPartition = topicPartition;
        }

        public boolean isLocal() {
            return maybeNode.isEmpty();
        }

        public Option<Node> node() {
            return maybeNode;
        }

        public TopicPartition topicPartition() {
            return topicPartition;
        }

        @Override
        public String toString() {
            return "MaybeLocalTopicPartition{" +
                    "maybeNode=" + maybeNode +
                    ", topicPartition=" + topicPartition +
                    '}';
        }
    }

}
