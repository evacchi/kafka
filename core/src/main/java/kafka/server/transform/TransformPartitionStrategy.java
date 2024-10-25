package kafka.server.transform;

import org.apache.kafka.common.message.MetadataResponseData;
import org.apache.kafka.common.record.SimpleRecord;

import java.util.List;

@FunctionalInterface
public interface TransformPartitionStrategy {
    MaybeLocalTopicPartition pick(
            String outputTopic,
            SimpleRecord record,
            List<MetadataResponseData.MetadataResponsePartition> partitions);


}
