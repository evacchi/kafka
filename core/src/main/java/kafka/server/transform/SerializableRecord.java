package kafka.server.transform;

import org.apache.kafka.common.record.Record;
import org.apache.kafka.common.record.SimpleRecord;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;

class SerializableRecord {

    public transient long timestamp;
    public String topic;
    public ByteBuffer key;
    public ByteBuffer value;
    public List<Header> headers;

    SerializableRecord() {}

    public SerializableRecord(Record sr) {
        this.timestamp = sr.timestamp();
        this.key = sr.key();
        this.value = sr.value();
        this.headers = extractHeaders(sr.headers());
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

    private static List<Header> extractHeaders(org.apache.kafka.common.header.Header[] headers) {
        var l = new ArrayList<Header>();
        for (var h : headers) {
            l.add(new Header(h.key(), new String(h.value(), StandardCharsets.UTF_8)));
        }
        return l;
    }

    public SimpleRecord toSimpleRecord() {
        return new SimpleRecord(
                this.timestamp, this.key, this.value /* fixme: missing headers */);
    }
}

