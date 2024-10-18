package kafka.server.transform;

import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.MapperFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.fasterxml.jackson.databind.json.JsonMapper;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.compress.Compression;
import org.apache.kafka.common.record.MemoryRecords;
import org.apache.kafka.common.record.SimpleRecord;

import java.io.FileInputStream;
import java.io.IOException;
import java.util.ArrayList;

public class TransformStore {

    Transform ktransform;
    ObjectMapper mapper;

    public TransformStore() throws IOException {
        ktransform = Transform.fromInputStream("my-first-plugin",
                new FileInputStream("/Users/evacchi/Devel/dylibso/xtp-demo/plugins/upper/dist/plugin.wasm"));
        mapper = JsonMapper.builder()
                .disable(SerializationFeature.FAIL_ON_EMPTY_BEANS)
                .disable(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES)
                .enable(MapperFeature.AUTO_DETECT_FIELDS).build();
    }

    public MemoryRecords transform(TopicPartition topicPartition, MemoryRecords inputRecords) {
        ArrayList<SimpleRecord> results = new ArrayList<>();

        for (org.apache.kafka.common.record.Record record : inputRecords.records()) {
            var in = new kafka.server.transform.Record(
                    topicPartition.topic(),
                    record.key(),
                    record.value(),
                    record.timestamp(),
                    record.headers());
            for (kafka.server.transform.Record out : ktransform.transform(in, mapper)) {
                var sr = new SimpleRecord(out.timestamp(), out.key(), out.value());
                results.add(sr);
            }
        }

        return MemoryRecords.withRecords(
                Compression.NONE, results.toArray(SimpleRecord[]::new));

    }

}
