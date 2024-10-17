package kafka.server.transform;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.common.compress.Compression;
import org.apache.kafka.common.record.MemoryRecords;
import org.apache.kafka.common.record.SimpleRecord;

import java.util.ArrayList;
import java.util.List;

public class RecordBatches {
    public static List<Record> fromMemoryRecords(String topic, MemoryRecords v) {
        ArrayList<Record> records = new ArrayList<>();
        for (var batch : v.batches()) {
            for (var record : batch) {
                records.add(new Record(topic, record));
            }
        }
        return records;
    }

    public static MemoryRecords fromRecords(
            String topic, MemoryRecords input, KafkaTransform kafkaTransform, ObjectMapper mapper) {
        SimpleRecord[] records = RecordBatches.fromMemoryRecords(topic, input).stream()
                .flatMap(r -> kafkaTransform.transform(r, mapper).stream())
                .map(r -> new SimpleRecord(r.timestamp(), r.key(), r.value())).toArray(SimpleRecord[]::new);
        return MemoryRecords.withRecords(Compression.NONE, records);
    }

}
