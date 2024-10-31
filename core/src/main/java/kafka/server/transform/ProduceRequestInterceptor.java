package kafka.server.transform;

import org.apache.kafka.common.Configurable;
import org.apache.kafka.common.requests.ProduceRequest;

public interface ProduceRequestInterceptor extends Configurable, AutoCloseable {
    void intercept(ProduceRequest produceRequest);
}
