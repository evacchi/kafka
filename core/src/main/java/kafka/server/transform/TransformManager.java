package kafka.server.transform;

import kafka.cluster.Partition;
import kafka.server.ActionQueue;
import kafka.server.KafkaConfig;
import kafka.server.MetadataCache;
import kafka.server.ReplicaManager;
import kafka.transform.TransformTypeConversions;
import org.apache.kafka.common.Node;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.compress.Compression;
import org.apache.kafka.common.message.MetadataResponseData;
import org.apache.kafka.common.metrics.Metrics;
import org.apache.kafka.common.record.MemoryRecords;
import org.apache.kafka.common.record.SimpleRecord;
import org.apache.kafka.common.utils.Time;
import org.apache.kafka.server.common.RequestLocal;
import org.apache.kafka.storage.internals.log.AppendOrigin;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import scala.Option;
import scala.collection.Seq;
import scala.runtime.BoxedUnit;

import java.io.FileInputStream;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.ConcurrentLinkedQueue;

import static java.util.stream.Collectors.toMap;

public class TransformManager {
    private static final Logger LOGGER = LoggerFactory.getLogger(TransformManager.class);

    private final ReplicaManager replicaManager;
    private final MetadataCache metadataCache;
    private final KafkaConfig config;

    private final ConcurrentLinkedQueue<Transform> ktransform = new ConcurrentLinkedQueue<>();
    private final TransformPartitionStrategy partitionStrategy;
    private final TransformChannelManager transformChannelManager;

    public TransformManager(
            ReplicaManager replicaManager,
            MetadataCache metadataCache,
            KafkaConfig config,
            Metrics metrics,
            Time time) {
        this.replicaManager = replicaManager;
        this.metadataCache = metadataCache;
        this.config = config;
        this.transformChannelManager =
                TransformChannelManager.create(config, metrics, time);
        this.partitionStrategy = this.nth(2);
    }

    public void start() {
        transformChannelManager.start();

        String pluginName = "upper";
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

        var responses = getTopicMetadata(Set.of(inputTopic, outputTopic));

        LOGGER.info("Transform '{}': Metadata = {}", pluginName, responses);
        if (!responses.containsKey(inputTopic)) {
            LOGGER.error(
                    "Transform '{}': Could not find input topic '{}'. Transform will not be instantiated.",
                    pluginName, inputTopic);
        } else if (!responses.containsKey(outputTopic)) {
            LOGGER.error(
                    "Transform '{}': Could not find output topic '{}'. Transform will not be instantiated.",
                    pluginName, outputTopic);
        } else {
            var resp = responses.get(outputTopic);
            resp.partitions().get(0).leaderId();
            for (MetadataResponseData.MetadataResponsePartition part : resp.partitions()) {
                Option<Node> partitionLeaderEndpoint = metadataCache.getPartitionLeaderEndpoint(
                        outputTopic, part.leaderId(), config.interBrokerListenerName());
                LOGGER.info("Transform '{}': Partition {} Leader Endpoint = '{}'",
                        pluginName, part.partitionIndex(), partitionLeaderEndpoint);
            }
            try {
                Transform transform = Transform.fromManifest(manifest);
                ktransform.add(transform);
                LOGGER.info("Transform '{}': Successfully initialized.", pluginName);
            } catch (IOException e) {
                LOGGER.error("Transform '" + pluginName + "': An error was caught at init time.", e);
            }
        }
    }

    private Map<String, MetadataResponseData.MetadataResponseTopic> getTopicMetadata(Set<String> topics) {
        var set = TransformTypeConversions.asScalaSet(topics);
        var topicMetadata = metadataCache.getTopicMetadata(
                set, config.interBrokerListenerName(),
                false,
                false);
        return TransformTypeConversions.metadataSeqToJavaMap(topicMetadata);
    }

    public void transformBatch(Map<TopicPartition, MemoryRecords> batch, long localAppendTimeout, ActionQueue actionQueue) {
        var transformed = batch.entrySet().stream()
                .flatMap(e -> transform(e.getKey(), e.getValue()).entrySet().stream())
                .collect(toMap(Map.Entry::getKey, Map.Entry::getValue));

        var local = transformed.entrySet().stream().filter(e -> e.getKey().isLocal())
                .collect(toMap(e -> e.getKey().topicPartition(), Map.Entry::getValue));
        var nonlocal = transformed.entrySet().stream().filter(e -> !e.getKey().isLocal())
                .collect(toMap(Map.Entry::getKey, Map.Entry::getValue));


        if (!local.isEmpty()) {
            replicaManager.appendRecords(
                    localAppendTimeout,
                    (short) -1,
                    false,
                    AppendOrigin.CLIENT,
                    TransformTypeConversions.asScalaMap(local),
                    m -> {
                        LOGGER.info("response callback: {}", m);
                        return BoxedUnit.UNIT;
                    },
                    Option.empty(),
                    m -> {
                        LOGGER.info("validation callback: {}", m);
                        return BoxedUnit.UNIT;
                    },
                    RequestLocal.noCaching(),
                    actionQueue,
                    TransformTypeConversions.asScalaMap(Map.of()));
        }

        if (!nonlocal.isEmpty()) {
            for (var e : nonlocal.entrySet()) {
                transformChannelManager.enqueue(
                        e.getKey().node().get(),
                        e.getKey().topicPartition(),
                        e.getValue());
            }
            transformChannelManager.wakeup();
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
                    // Each transform may result in multiple records
                    Collection<SimpleRecord> records = transf.transform(in);
                    // For each record, create a SimpleRecord and compute a valid partition.
                    for (var out : records) {
                        MaybeLocalTopicPartition destTopicPartition =
                                findPartition(transf.manifest().outputTopic(), out);
                        transformedRecords.computeIfAbsent(
                                destTopicPartition,
                                k -> new ArrayList<>()).add(out);
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

    /**
     * Prefer a local partition for the output topic. If such a partition is not found
     */
    private MaybeLocalTopicPartition findPartition(String outputTopic, SimpleRecord sr) {
        var responses = getTopicMetadata(Set.of(outputTopic));
        var partitions = responses.get(outputTopic).partitions();
        return partitionStrategy.pick(outputTopic, sr, partitions);
    }


    TransformPartitionStrategy random() {
        Random r = new Random();
        return (topic, sr, partitions) -> {
            int partIdx = r.nextInt(partitions.size());
            var meta = partitions.stream().filter(p -> p.partitionIndex() == partIdx).findFirst().get();
            return findPartitionForMetadata(topic, meta);
        };
    }

    TransformPartitionStrategy nth(int n) {
        Random r = new Random();
        return (topic, sr, partitions) -> {
            var meta = partitions.stream().filter(p -> p.partitionIndex() == n).findFirst().get();
            return findPartitionForMetadata(topic, meta);
        };
    }


    private MaybeLocalTopicPartition findPartitionForMetadata(String topic, MetadataResponseData.MetadataResponsePartition meta) {
        Option<Node> partitionLeaderEndpoint = metadataCache.getPartitionLeaderEndpoint(
                topic, meta.leaderId(), config.interBrokerListenerName());
        if (partitionLeaderEndpoint.isDefined()) {
            return new MaybeLocalTopicPartition(
                    partitionLeaderEndpoint,
                    new TopicPartition(topic, meta.partitionIndex()));
        } else {
            Option<Partition> localOnlinePartition = replicaManager.onlinePartition(new TopicPartition(topic, meta.partitionIndex()));
            if (localOnlinePartition.isDefined()) {
                var p = localOnlinePartition.get();
                return new MaybeLocalTopicPartition(Option.empty(), new TopicPartition(p.topic(), p.partitionId()));
            } else {
                throw new UnsupportedOperationException("FIXME: could not find any partition for the given topic. " +
                        topic + " This should have been validated earlier.");
            }
        }
    }


    static class BatchResult {
        private final Map<TopicPartition, MemoryRecords> local;
        private final Map<MaybeLocalTopicPartition, MemoryRecords> nonlocal;

        public BatchResult(
                Map<TopicPartition, MemoryRecords> local,
                Map<MaybeLocalTopicPartition, MemoryRecords> nonlocal) {
            this.local = local;
            this.nonlocal = nonlocal;
        }

        public Map<TopicPartition, MemoryRecords> local() {
            return local;
        }

        public Map<MaybeLocalTopicPartition, MemoryRecords> nonlocal() {
            return nonlocal;
        }
    }


}
