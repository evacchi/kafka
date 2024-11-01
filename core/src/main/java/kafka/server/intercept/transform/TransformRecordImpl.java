package kafka.server.intercept.transform;

import kafka.server.intercept.ProduceRequestInterceptor;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.header.Header;
import org.apache.kafka.common.record.Record;

import java.nio.ByteBuffer;

public class TransformRecordImpl implements ProduceRequestInterceptor.Record {
    TopicPartition topicPartition;
    long timestamp;
    ByteBuffer key;
    ByteBuffer value;
    Header[] headers;

    TransformRecordImpl(String topic, int partition, long timestamp, ByteBuffer key, ByteBuffer value, Header[] headers) {
        this.topicPartition = new TopicPartition(topic, partition);
        this.timestamp = timestamp;
        this.key = key;
        this.value = value;
        this.headers = headers;
    }

    @Override
    public TopicPartition topicPartition() {
        return topicPartition;
    }

    @Override
    public long timestamp() {
        return timestamp;
    }

    @Override
    public ByteBuffer key() {
        return key;
    }

    @Override
    public ByteBuffer value() {
        return value;
    }

    @Override
    public Header[] headers() {
        return headers;
    }
}

