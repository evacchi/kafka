package kafka.server.transform;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.MapperFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.fasterxml.jackson.databind.json.JsonMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.apache.kafka.common.header.Header;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

public class InternalMapper {
    private static final ObjectMapper MAPPER = JsonMapper.builder()
            .disable(SerializationFeature.FAIL_ON_EMPTY_BEANS)
            .disable(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES)
            .enable(MapperFeature.AUTO_DETECT_FIELDS).build();


    public static byte[] asBytes(ProduceRequestInterceptor.Record sr) {
        try {
            ObjectNode jsonNodes = new ObjectNode(MAPPER.getNodeFactory())
                    .put("timestamp", sr.timestamp())
                    .put("topic", sr.topicPartition().topic())
                    .put("partition", sr.topicPartition().partition())
                    .putPOJO("key", sr.key())
                    .putPOJO("value", sr.value())
                    .putPOJO("headers", sr.headers());

            return MAPPER.writeValueAsBytes(jsonNodes);

//            return MAPPER.writeValueAsBytes(new SerializableRecord(sr));
        } catch (JsonProcessingException e) {
            throw new UncheckedIOException(e);
        }
    }

    public static Collection<? extends ProduceRequestInterceptor.Record> fromBytes(String topic, byte[] src) {
        try {
            ArrayList<ProduceRequestInterceptor.Record> records = new ArrayList<>();
            for (JsonNode jsonNode : MAPPER.readTree(src)) {
                long timestamp = jsonNode.has("timestamp") ? jsonNode.get("timestamp").asLong() : 0L;
                ByteBuffer key = jsonNode.has("key") ? MAPPER.convertValue(jsonNode.get("key"), ByteBuffer.class) : ByteBuffer.allocate(0);
                ByteBuffer value = jsonNode.has("value") ? MAPPER.convertValue(jsonNode.get("value"), ByteBuffer.class) : ByteBuffer.allocate(0);
//                Header[] headers = jsonNode.has("headers") ? MAPPER.convertValue(jsonNode.get("headers"), Header[].class) : new Header[0];
                records.add(new RecordProxy(topic, 0, timestamp, key, value, new Header[0]));
            }
            return records;
//
//            return MAPPER.readValue(src,
//                    new TypeReference<List<RecordProxy>>() {
//                    });
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }
}
