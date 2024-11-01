package kafka.server.intercept;

import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.compress.Compression;
import org.apache.kafka.common.message.ProduceRequestData;
import org.apache.kafka.common.record.MemoryRecords;
import org.apache.kafka.common.record.SimpleRecord;
import org.apache.kafka.common.requests.ProduceRequest;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

class ProduceRequestInterceptorManager_ implements AutoCloseable {
    private static final Logger LOGGER = LoggerFactory.getLogger(ProduceRequestInterceptorManager_.class);
    private final ProduceRequestInterceptor interceptor;

    public ProduceRequestInterceptorManager_(ProduceRequestInterceptor interceptor) {
        this.interceptor = interceptor;
    }

    public void intercept(ProduceRequest request) {
        try (var svc = Executors.newSingleThreadExecutor()) {
            var results = new ConcurrentLinkedQueue<ProduceRequestInterceptor.Record>();

            ProduceRequestData requestData = request.data();

            ProduceRequestData.TopicProduceDataCollection topicProduceData = requestData.topicData();
            for (ProduceRequestData.TopicProduceData tpd : topicProduceData) {
                for (ProduceRequestData.PartitionProduceData ppd : tpd.partitionData()) {
                    MemoryRecords mr = (MemoryRecords) ppd.records();
                    for (var record : mr.records()) {
                        RecordImpl in = new RecordImpl(tpd.name(), ppd.index(), record);
                        svc.submit(() -> {
                            var records = interceptor.intercept(in);
                            results.addAll(records);
                        });
                    }
                }
            }

            svc.shutdown();
            svc.awaitTermination(1, TimeUnit.SECONDS);

            var pending = new HashMap<TopicPartition, ArrayList<SimpleRecord>>();
            for (var result : results) {
                pending.computeIfAbsent(
                                result.topicPartition(), k -> new ArrayList<>())
                        .add(Conversions.asSimpleRecord(result));
            }
            var tpd = mergeTopicProduceData(topicProduceData, pending);
            request.data().setTopicData(tpd);
        } catch (InterruptedException e) {
            LOGGER.warn("Execution timed out", e);
        }
    }

    private ProduceRequestData.TopicProduceDataCollection mergeTopicProduceData(
            ProduceRequestData.TopicProduceDataCollection topicProduceData,
            HashMap<TopicPartition, ArrayList<SimpleRecord>> pending) {

        var tpd = new ProduceRequestData.TopicProduceDataCollection(pending.size());

        for (ProduceRequestData.TopicProduceData el : topicProduceData) {
            tpd.add(el.duplicate());
        }

        for (var kv : pending.entrySet()) {
            TopicPartition tp = kv.getKey();
            SimpleRecord[] records = kv.getValue().toArray(SimpleRecord[]::new);
            tpd.add(new ProduceRequestData.TopicProduceData()
                    .setName(tp.topic())
                    .setPartitionData(List.of(
                            new ProduceRequestData.PartitionProduceData()
                                    .setIndex(tp.partition())
                                    .setRecords(MemoryRecords.withRecords(Compression.NONE, records)))));
        }

        return tpd;
    }


    @Override
    public void close() throws Exception {
        interceptor.close();
    }

}
