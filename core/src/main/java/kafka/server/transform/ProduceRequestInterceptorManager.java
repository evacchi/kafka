package kafka.server.transform;

import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.compress.Compression;
import org.apache.kafka.common.message.ProduceRequestData;
import org.apache.kafka.common.record.MemoryRecords;
import org.apache.kafka.common.record.SimpleRecord;
import org.apache.kafka.common.requests.ProduceRequest;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;

public class ProduceRequestInterceptorManager implements AutoCloseable {
    private static final Logger LOGGER = LoggerFactory.getLogger(ProduceRequestInterceptorManager.class);
    private final ProduceRequestInterceptor interceptor;
    private Duration timeout;

    public ProduceRequestInterceptorManager(ProduceRequestInterceptor interceptor) {
        this.interceptor = interceptor;
        this.timeout = Duration.of(100, ChronoUnit.MILLIS);
    }

    public void intercept(ProduceRequest request) {
        var pending = new HashMap<TopicPartition, ArrayList<SimpleRecord>>();

        ProduceRequestData.TopicProduceDataCollection topicProduceData = request.data().topicData();
        for (ProduceRequestData.TopicProduceData tpd : topicProduceData) {
            for (ProduceRequestData.PartitionProduceData ppd : tpd.partitionData()) {
                MemoryRecords mr = (MemoryRecords) ppd.records();
                for (var record : mr.records()) {
                    try {
                        Collection<? extends ProduceRequestInterceptor.Record> results =
                                interceptor.intercept(
                                        new RecordProxy(tpd.name(), ppd.index(), record), timeout);

                        results.forEach(r -> LOGGER.info(r.topicPartition().toString()));

                        if (!results.isEmpty()) {
                            for (ProduceRequestInterceptor.Record result : results) {
                                ArrayList<SimpleRecord> collector = pending.computeIfAbsent(
                                        result.topicPartition(), k -> new ArrayList<>());
                                collector.add(Conversions.asSimpleRecord(result));
                            }
                        }
                    } catch (ProduceRequestInterceptor.InterceptTimeoutException e) {
                        LOGGER.warn("Interceptor failed. Batch skipped.", e);
                    }
                }
            }
        }

        var tpd = new ProduceRequestData.TopicProduceDataCollection(
                topicProduceData.size() + pending.size());

        for (ProduceRequestData.TopicProduceData el : topicProduceData) {
            tpd.add(el.duplicate());
        }

        for (var kv : pending.entrySet()) {
            TopicPartition tp = kv.getKey();
            tpd.add(new ProduceRequestData.TopicProduceData()
                    .setName(tp.topic())
                    .setPartitionData(List.of(
                            new ProduceRequestData.PartitionProduceData()
                                    .setIndex(tp.partition())
                                    .setRecords(MemoryRecords.withRecords(
                                            Compression.NONE,
                                            kv.getValue().toArray(SimpleRecord[]::new))))));
        }
        request.data().setTopicData(tpd);
    }


    @Override
    public void close() throws Exception {
        interceptor.close();
    }

}
