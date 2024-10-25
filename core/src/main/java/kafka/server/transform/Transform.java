package kafka.server.transform;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.extism.chicory.sdk.Manifest;
import org.extism.chicory.sdk.ManifestWasm;
import org.extism.chicory.sdk.Plugin;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.List;

public class Transform {
    public static Transform fromManifest(
            TransformManifest manifest) throws IOException {
        ManifestWasm wasm = ManifestWasm.fromBytes(manifest.inputStream.readAllBytes()).build();
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

    public List<Record> transform(Record record, ObjectMapper mapper) {
        try {
            return mapper.readValue(transformBytes(mapper.writeValueAsBytes(record)),
                    new TypeReference<List<Record>>() {});
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    public byte[] transformBytes(byte[] recordBytes) {
        return plugin.call("transform", recordBytes);
    }

    public TransformManifest manifest() {
        return manifest;
    }

}
