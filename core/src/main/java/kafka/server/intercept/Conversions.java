package kafka.server.intercept;

import org.apache.kafka.common.record.SimpleRecord;

class Conversions {
    static SimpleRecord asSimpleRecord(ProduceRequestInterceptor.Record record) {
        return new SimpleRecord(System.nanoTime(), record.key(), record.value(), record.headers());
    }
}
