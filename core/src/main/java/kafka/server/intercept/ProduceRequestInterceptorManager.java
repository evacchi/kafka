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
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;

public class ProduceRequestInterceptorManager implements AutoCloseable {
    private static final Logger LOGGER = LoggerFactory.getLogger(ProduceRequestInterceptorManager.class);
    private final ProduceRequestInterceptor interceptor;

    public ProduceRequestInterceptorManager(ProduceRequestInterceptor interceptor) {
        this.interceptor = interceptor;
    }

    public void intercept(ProduceRequest request) {
        var futureResults = new ArrayList<Future<Collection<? extends ProduceRequestInterceptor.Record>>>();

        ProduceRequestData.TopicProduceDataCollection topicProduceData = request.data().topicData();
        for (ProduceRequestData.TopicProduceData tpd : topicProduceData) {
            for (ProduceRequestData.PartitionProduceData ppd : tpd.partitionData()) {
                MemoryRecords mr = (MemoryRecords) ppd.records();
                for (var record : mr.records()) {
                    var future =
                            interceptor.intercept(
                                    new RecordImpl(tpd.name(), ppd.index(), record));

                    futureResults.add(future);
                }
            }
        }



        var pending = new HashMap<TopicPartition, ArrayList<SimpleRecord>>();
        for (var future : futureResults) {
            try {
            Collection<? extends ProduceRequestInterceptor.Record> result = future.get();
                pending.computeIfAbsent(
                                result.topicPartition(), k -> new ArrayList<>())
                        .add(Conversions.asSimpleRecord(result));

            } catch (InterruptedException | ExecutionException e) {
                LOGGER.warn("Execution timed out, skipping result.", e);
            }
        }


        var tpd = mergeTopicProduceData(topicProduceData, pending);
        request.data().setTopicData(tpd);
    }

    private ProduceRequestData.TopicProduceDataCollection mergeTopicProduceData(
            ProduceRequestData.TopicProduceDataCollection topicProduceData,
            HashMap<TopicPartition, ArrayList<SimpleRecord>> pending) {

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
        return tpd;
    }


    @Override
    public void close() throws Exception {
        interceptor.close();
    }

}
