package kafka.server.transform;

import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.header.Header;
import org.apache.kafka.common.record.Record;

import java.nio.ByteBuffer;

class RecordProxy implements ProduceRequestInterceptor.Record {
    TopicPartition topicPartition;
    long timestamp;
    ByteBuffer key;
    ByteBuffer value;
    Header[] headers;

    RecordProxy(){}

    RecordProxy(String topic, int partition, Record original) {
        this(topic, partition,
                original.timestamp(),
                original.key(),
                original.value(),
                original.headers());
    }

    RecordProxy(String topic, int partition, long timestamp, ByteBuffer key, ByteBuffer value, Header[] headers) {
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

