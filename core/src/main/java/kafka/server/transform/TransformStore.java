package kafka.server.transform;

import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.MapperFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.fasterxml.jackson.databind.json.JsonMapper;
import kafka.server.KafkaConfig;
import kafka.server.MetadataCache;
import kafka.transform.TransformTypeConversions;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.compress.Compression;
import org.apache.kafka.common.message.MetadataResponseData;
import org.apache.kafka.common.record.MemoryRecords;
import org.apache.kafka.common.record.SimpleRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.FileInputStream;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

public class TransformStore {
    ScheduledExecutorService scheduler = Executors.newSingleThreadScheduledExecutor();

    private static final Logger LOGGER = LoggerFactory.getLogger(TransformStore.class);

    private final ObjectMapper mapper;
    private final MetadataCache metadataCache;
    private final KafkaConfig config;

    private final ConcurrentLinkedQueue<Transform> ktransform = new ConcurrentLinkedQueue<>();

    public TransformStore(MetadataCache metadataCache, KafkaConfig config) {
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
//            var resp = responses.get(outputTopic);
//            resp.partitions().get(0).leaderId()
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
    public Map<TopicPartition, MemoryRecords> transform(TopicPartition topicPartition, MemoryRecords inputRecords) {
        Map<TopicPartition, ArrayList<SimpleRecord>> transformedRecords = new HashMap<>();

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
                        int partition = computePartition(sr);
                        transformedRecords.computeIfAbsent(
                                new TopicPartition(manifest.outputTopic(), partition),
                                k -> new ArrayList<>()).add(sr);
                    }
                }
            }
        }

        // Once we collect all the results, we can create one MemoryRecords for each TopicPartition
        Map<TopicPartition, MemoryRecords> resultMap = new HashMap<>();
        for (var pair : transformedRecords.entrySet()) {
            var results = pair.getValue();
            MemoryRecords memoryRecords = MemoryRecords.withRecords(
                    Compression.NONE, results.toArray(SimpleRecord[]::new));
            resultMap.put(pair.getKey(), memoryRecords);
        }

        return resultMap;
    }

    // TODO
    private int computePartition(SimpleRecord sr) {
        return 2;
    }

}
