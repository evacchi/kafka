package kafka.server.transform;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.MapperFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.fasterxml.jackson.databind.json.JsonMapper;
import org.apache.kafka.common.record.Record;
import org.apache.kafka.common.record.SimpleRecord;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.List;
import java.util.stream.Collectors;

public class InternalMapper {
    private static final ObjectMapper MAPPER = JsonMapper.builder()
            .disable(SerializationFeature.FAIL_ON_EMPTY_BEANS)
            .disable(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES)
            .enable(MapperFeature.AUTO_DETECT_FIELDS).build();


    public static byte[] asBytes(Record sr) {
        try {
            return MAPPER.writeValueAsBytes(new SerializableRecord(sr));
        } catch (JsonProcessingException e) {
            throw new UncheckedIOException(e);
        }
    }

    public static List<SimpleRecord> fromBytes(byte[] src) {
        try {
            List<SerializableRecord> serializableRecords = MAPPER.readValue(src,
                    new TypeReference<List<SerializableRecord>>() {});
            return serializableRecords.stream()
                    .map(SerializableRecord::toSimpleRecord)
                    .collect(Collectors.toList());
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }
}
