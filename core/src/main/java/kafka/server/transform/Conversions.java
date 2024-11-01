package kafka.server.transform;

import org.apache.kafka.common.record.SimpleRecord;

class Conversions {
    static SimpleRecord asSimpleRecord(ProduceRequestInterceptor.Record record) {
        return new SimpleRecord(record.timestamp(), record.key(), record.value(), record.headers());
    }
}
