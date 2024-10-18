package kafka.server.transform;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;

public class Record {

    public transient long timestamp;
    public String topic;
    public ByteBuffer key;
    public ByteBuffer value;
    public List<Header> headers;

    public Record() {
    }

    public Record(
            String topic,
            ByteBuffer key,
            ByteBuffer value,
            long timestamp,
            org.apache.kafka.common.header.Header[] headers) {
        this.topic = topic;
        this.timestamp = timestamp;
        this.key = key;
        this.value = value;
        this.headers = extractHeaders(headers);
    }

    public long timestamp() {
        return timestamp;
    }

    public ByteBuffer key() {
        return key;
    }

    public ByteBuffer value() {
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

