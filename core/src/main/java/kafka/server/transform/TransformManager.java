package kafka.server.transform;

import org.apache.kafka.common.compress.Compression;
import org.apache.kafka.common.message.ProduceRequestData;
import org.apache.kafka.common.record.MemoryRecords;
import org.apache.kafka.common.record.SimpleRecord;
import org.apache.kafka.common.requests.ProduceRequest;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.FileInputStream;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.time.Duration;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

public class TransformManager implements ProduceRequestInterceptor{
    private static final Logger LOGGER = LoggerFactory.getLogger(TransformManager.class);

    private final ConcurrentLinkedQueue<Transform> ktransform = new ConcurrentLinkedQueue<>();
    private final ExecutorService executorService = Executors.newCachedThreadPool();

    public TransformManager() {}

    @Override
    public void configure(Map<String, ?> configs) {
        String pluginName = "upper";
        String inputTopic = "test";
        String outputTopic = "test";
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

        try {
            Transform transform = Transform.fromManifest(manifest, executorService);
            ktransform.add(transform);
            LOGGER.info("Transform '{}': Successfully initialized.", pluginName);
        } catch (IOException e) {
            LOGGER.error("Transform '" + pluginName + "': An error was caught at init time.", e);
        }
    }

    public void intercept(ProduceRequest request) {
        var pending = new HashMap<String, ArrayList<SimpleRecord>>();

        ProduceRequestData.TopicProduceDataCollection topicProduceData = request.data().topicData();
        for (ProduceRequestData.TopicProduceData tpd : topicProduceData) {
            for (ProduceRequestData.PartitionProduceData ppd : tpd.partitionData()) {
                MemoryRecords mr = (MemoryRecords) ppd.records();
                for (var batch : mr.batches()) {
                    for (var record : batch) {
                        for (Transform transf : ktransform) {
                            try {
                                Collection<SimpleRecord> result = transf.transform(record,
                                        Duration.of(1, ChronoUnit.MILLIS));
                                if (!result.isEmpty()) {
                                    pending.computeIfAbsent(
                                            transf.manifest().outputTopic(),
                                            k -> new ArrayList<>()).addAll(result);
                                }
                            } catch (InterruptedException | ExecutionException | TimeoutException e) {
                                LOGGER.warn("An error was thrown while performing the transform", e);
                            }
                        }
                    }
                }
            }
        }

        var tpd = new ProduceRequestData.TopicProduceDataCollection(
                topicProduceData.size() + pending.size());
        // FIXME: should be fused
        tpd.addAll(topicProduceData);

        for (var kv : pending.entrySet()) {
            tpd.add(new ProduceRequestData.TopicProduceData()
                    .setName(kv.getKey())
                    .setPartitionData(List.of(
                            new ProduceRequestData.PartitionProduceData()
                                    .setRecords(MemoryRecords.withRecords(
                                            Compression.NONE,
                                            kv.getValue().toArray(SimpleRecord[]::new))))));
        }
        request.data().setTopicData(tpd);
    }


    @Override
    public void close() throws Exception {
        executorService.close();
    }

}
