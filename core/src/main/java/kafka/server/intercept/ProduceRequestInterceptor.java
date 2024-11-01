package kafka.server.intercept;

import org.apache.kafka.common.Configurable;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.header.Header;

import java.nio.ByteBuffer;
import java.util.Collection;
import java.util.concurrent.Future;

public interface ProduceRequestInterceptor extends Configurable, AutoCloseable {
    interface Record {

        TopicPartition topicPartition();

        long timestamp();

        /**
         * Get the record's key.
         *
         * @return the key or null if there is none
         */
        ByteBuffer key();

        /**
         * Get the record's value
         *
         * @return the (nullable) value
         */
        ByteBuffer value();

        /**
         * Get the headers. For magic versions 1 and below, this always returns an empty array.
         *
         * @return the array of headers
         */
        Header[] headers();

    }

    class InterceptTimeoutException extends Exception {
        public InterceptTimeoutException(String message, Throwable cause) {
            super(message, cause);
        }
    }

    Future<Collection<? extends Record>> intercept(Record record);
}
