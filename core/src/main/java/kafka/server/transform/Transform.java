package kafka.server.transform;

import org.apache.kafka.common.record.SimpleRecord;
import org.extism.chicory.sdk.Manifest;
import org.extism.chicory.sdk.ManifestWasm;
import org.extism.chicory.sdk.Plugin;

import java.io.IOException;
import java.time.Duration;
import java.util.Collection;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

public class Transform {
    public static Transform fromManifest(
            TransformManifest manifest, ExecutorService executorService) throws IOException {
        ManifestWasm wasm = ManifestWasm.fromBytes(
                manifest.inputStream.readAllBytes()).build();
        Plugin plugin = Plugin.ofManifest(Manifest.ofWasms(wasm).build())
                .build();
        return new Transform(plugin, manifest, executorService);
    }

    private final Plugin plugin;
    private final TransformManifest manifest;
    private final ExecutorService executorService;

    public Transform(Plugin plugin,
                     TransformManifest manifest,
                     ExecutorService executorService) {
        this.plugin = plugin;
        this.manifest = manifest;
        this.executorService = executorService;
    }

    public Collection<SimpleRecord> transform(org.apache.kafka.common.record.Record record, Duration duration)
            throws ExecutionException, InterruptedException, TimeoutException {
        Future<Collection<SimpleRecord>> result = executorService.submit(() -> transform(record));
        return result.get(duration.toNanos(), TimeUnit.NANOSECONDS);
    }

    public Collection<SimpleRecord> transform(org.apache.kafka.common.record.Record record) {
        byte[] in = InternalMapper.asBytes(record);
        byte[] out = transformBytes(in);
        return InternalMapper.fromBytes(out);
    }

    public byte[] transformBytes(byte[] recordBytes) {
        return plugin.call("transform", recordBytes);
    }

    public TransformManifest manifest() {
        return manifest;
    }

}
