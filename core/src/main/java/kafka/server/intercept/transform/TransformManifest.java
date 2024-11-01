package kafka.server.intercept.transform;

import java.io.InputStream;
import java.util.Map;

public class TransformManifest {
    final InputStream inputStream;
    private final String name;
    private final String inputTopic;
    private final String outputTopic;
    private final Map<String, String> config;

    public TransformManifest(InputStream inputStream,
                             String name,
                             String inputTopic,
                             String outputTopic,
                             Map<String, String> config) {
        this.inputStream = inputStream;
        this.name = name;
        this.inputTopic = inputTopic;
        this.outputTopic = outputTopic;
        this.config = config;
    }

    public Map<String, String> config() {
        return config;
    }

    public String outputTopic() {
        return outputTopic;
    }

    public String inputTopic() {
        return inputTopic;
    }

    public String name() {
        return name;
    }
}
