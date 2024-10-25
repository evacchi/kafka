package kafka.server.transform;

import org.apache.kafka.common.record.SimpleRecord;
import org.extism.chicory.sdk.Manifest;
import org.extism.chicory.sdk.ManifestWasm;
import org.extism.chicory.sdk.Plugin;

import java.io.IOException;
import java.util.Collection;

public class Transform {
    public static Transform fromManifest(
            TransformManifest manifest) throws IOException {
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
