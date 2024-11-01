package kafka.server.intercept;

import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.header.Header;
import org.apache.kafka.common.record.Record;

import java.nio.ByteBuffer;

public class RecordImpl implements ProduceRequestInterceptor.Record {
    private final TopicPartition topicPartition;
    private final long timestamp;
    private final ByteBuffer key;
    private final ByteBuffer value;
    private final Header[] headers;

    RecordImpl(String topic, int partition, Record original) {
        this.topicPartition = new TopicPartition(topic, partition);
        this.timestamp = original.timestamp();
        this.key = original.key();
        this.value = original.value();
        this.headers = original.headers();
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

