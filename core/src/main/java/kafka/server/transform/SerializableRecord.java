package kafka.server.transform;

import org.apache.kafka.common.record.Record;
import org.apache.kafka.common.record.SimpleRecord;

import java.nio.ByteBuffer;

/**
 * This is only for PoC.
 * <p>
 * We use a simple class to (de)serialize a SimpleRecord using Jackson.
 * It can be easily done explicitly using the ObjectMapper directly,
 * ...assuming we want to use JSON as a format at all!
 */
class SerializableRecord {

    public transient long timestamp;
    public String topic;
    public ByteBuffer key;
    public ByteBuffer value;

    SerializableRecord() {}

    public SerializableRecord(Record sr) {
        this.timestamp = sr.timestamp();
        this.key = sr.key();
        this.value = sr.value();
    }

    long timestamp() {
        return timestamp;
    }

    ByteBuffer key() {
        return key;
    }

    ByteBuffer value() {
        return value;
    }

    public SimpleRecord toSimpleRecord() {
        return new SimpleRecord(
                this.timestamp, this.key, this.value /* fixme: missing headers */);
    }
}

