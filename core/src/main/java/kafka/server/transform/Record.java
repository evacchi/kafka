package kafka.server.transform;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;

public class Record {

    public transient long timestamp;
    public String topic;
    public byte[] key;
    public byte[] value;
    public List<Header> headers;

    public Record() {}

    public Record(String topic, org.apache.kafka.common.record.Record r) {
        this.topic = topic;
        this.timestamp = r.timestamp();
        this.key = keyToArray(r);
        this.value = valueToArray(r);
        this.headers = extractHeaders(r.headers());
    }

    private static byte[] keyToArray(org.apache.kafka.common.record.Record r) {
        if (r.keySize() > 0) {
            var bytes = new byte[r.keySize()];
            r.key().get(bytes);
            return bytes;
        } else return new byte[0];
    }

    private static byte[] valueToArray(org.apache.kafka.common.record.Record r) {
        if (r.valueSize() > 0) {
            var bytes = new byte[r.valueSize()];
            r.value().get(bytes);
            return bytes;
        } else return new byte[0];
    }

    public long timestamp() {
        return timestamp;
    }

    public byte[] key() {
        return key;
    }

    public byte[] value() {
        return value;
    }

    private static List<Header> extractHeaders(org.apache.kafka.common.header.Header[] headers) {
        var l = new ArrayList<Header>();
        for (var h : headers) {
            l.add(new Header(h.key(), new String(h.value(), StandardCharsets.UTF_8)));
        }
        return l;
    }

}

