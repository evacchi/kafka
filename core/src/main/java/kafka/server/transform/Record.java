package kafka.server.transform;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;

public class Record {
    public String topic;
    public byte[] key;
    public byte[] value;
    public List<Header> headers;

//    public static Record of(String topic, SimpleRecord r) throws IOException {
//        return new Record(
//                topic,
//                r.key().array(),
//                r.value().array(),
//                extractHeaders(Arrays.stream(r.headers()).toArray(org.apache.kafka.common.header.Header[]::new)));
//    }
//


    public Record() {
    }

    public Record(String topic, byte[] key, byte[] value, org.apache.kafka.common.header.Header[] headers) {
        this.topic = topic;
        this.key = key;
        this.value = value;
        this.headers = extractHeaders(headers);
    }

    public byte[] key() {
        return key;
    }

    public byte[] value() {
        return value;
    }

    //    public Record withHeaders(List<Header> headers) {
//        List<Header> h = this.headers == null ? new ArrayList<>() : new ArrayList<>(this.headers);
//        h.addAll(headers);
//        return new Record(topic, key, value, h);
//    }

    private static List<Header> extractHeaders(org.apache.kafka.common.header.Header[] headers) {
        var l = new ArrayList<Header>();
        for (var h : headers) {
            l.add(new Header(h.key(), new String(h.value(), StandardCharsets.UTF_8)));
        }
        return l;
    }

}

