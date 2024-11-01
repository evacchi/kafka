package kafka.server.transform;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.FileInputStream;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Map;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeoutException;

public class TransformManager implements ProduceRequestInterceptor {
    private static final Logger LOGGER = LoggerFactory.getLogger(TransformManager.class);

    private final ConcurrentLinkedQueue<Transform> ktransform = new ConcurrentLinkedQueue<>();
    private final ExecutorService executorService = Executors.newSingleThreadExecutor();

    public TransformManager() {
    }

    @Override
    public void configure(Map<String, ?> configs) {
        String pluginName = "upper";
        String inputTopic = "test";
        String outputTopic = "test-out";
        Map<String, String> config = Map.of();

        try {
            FileInputStream inputStream =
                    new FileInputStream("/Users/evacchi/Devel/dylibso/xtp-demo/plugins/upper/dist/plugin.wasm");
            var manifest = new TransformManifest(
                    inputStream, pluginName, inputTopic, outputTopic, config);
            registerTransform(manifest);
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    public void registerTransform(TransformManifest manifest) {
        String pluginName = manifest.name();

        try {
            Transform transform = Transform.fromManifest(manifest, executorService);
            ktransform.add(transform);
            LOGGER.info("Transform '{}': Successfully initialized.", pluginName);
        } catch (IOException e) {
            LOGGER.error("Transform '" + pluginName + "': An error was caught at init time.", e);
        }
    }

    @Override
    public Collection<? extends Record> intercept(Record record, Duration timeout) throws InterceptTimeoutException {
        ArrayList<Record> records = new ArrayList<Record>();
        for (Transform transf : ktransform) {
            try {
                records.addAll(transf.transform(record, timeout));
            } catch (InterruptedException | ExecutionException | TimeoutException e) {
                throw new InterceptTimeoutException("Transform exceeded timeout", e);
            }
        }
        return records;
    }


    @Override
    public void close() throws Exception {
        executorService.close();
    }

}
