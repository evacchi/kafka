package kafka.server.intercept.transform;

import kafka.server.intercept.ProduceRequestInterceptor;
import org.extism.chicory.sdk.Manifest;
import org.extism.chicory.sdk.ManifestWasm;
import org.extism.chicory.sdk.Plugin;

import java.io.IOException;
import java.time.Duration;
import java.util.Collection;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.TimeoutException;

public class Transform {
    public static Transform fromManifest(TransformManifest manifest) throws IOException {
        ManifestWasm wasm = ManifestWasm.fromBytes(
                manifest.inputStream.readAllBytes()).build();
        Plugin plugin = Plugin.ofManifest(Manifest.ofWasms(wasm).build())
                .build();
        return new Transform(plugin, manifest);
    }

    private final Plugin plugin;
    private final TransformManifest manifest;

    public Transform(Plugin plugin,
                     TransformManifest manifest) {
        this.plugin = plugin;
        this.manifest = manifest;
    }

    public Collection<? extends ProduceRequestInterceptor.Record> transform(ProduceRequestInterceptor.Record record) {
        byte[] in = InternalMapper.asBytes(record);
        byte[] out = transformBytes(in);
        return InternalMapper.fromBytes(manifest.outputTopic(), out);
    }

    public byte[] transformBytes(byte[] recordBytes) {
        return plugin.call("transform", recordBytes);
    }

    public TransformManifest manifest() {
        return manifest;
    }

}
