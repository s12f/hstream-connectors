package source.debezium;

import com.fasterxml.jackson.databind.JsonNode;
import io.hstream.io.KvStore;
import java.nio.ByteBuffer;
import java.util.*;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicReference;

import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.connect.json.JsonDeserializer;
import org.apache.kafka.connect.runtime.WorkerConfig;
import org.apache.kafka.connect.util.Callback;

@Slf4j
public class OffsetBackingStore implements org.apache.kafka.connect.storage.OffsetBackingStore {
    static KvStore store;
    static String namespace;
    static AtomicReference<List<JsonNode>> offsets = new AtomicReference<>(List.of());

    static public void setKvStore(KvStore kvStore) {
        store = kvStore;
    }

    static public void setNamespace(String namespace) {
        OffsetBackingStore.namespace = namespace;
    }

    @Override
    public void start() {}

    @Override
    public void stop() {}

    @Override
    public Future<Map<ByteBuffer, ByteBuffer>> get(Collection<ByteBuffer> keys) {
        var res = new HashMap<ByteBuffer, ByteBuffer>();
        for (var key : keys) {
            try {
                var keyStr = "offset_" + Base64.getEncoder().encodeToString(key.array());
                var val = store.get(keyStr).join();
                if (val != null) {
                    res.put(key, ByteBuffer.wrap(Base64.getDecoder().decode(val)));
                }
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        }
        var f = new CompletableFuture<Map<ByteBuffer, ByteBuffer>>();
        f.complete(res);
        return f;
    }

    @Override
    public Future<Void> set(Map<ByteBuffer, ByteBuffer> values, Callback<Void> callback) {
        var newOffsets = new ArrayList<JsonNode>(values.size());
        for (var entry : values.entrySet()) {
            JsonNode offset = null;
            try (var jc = new JsonDeserializer()) {
                offset = jc.deserialize(namespace, entry.getValue().array());
                newOffsets.add(offset);
            } catch (Exception e) {
                log.info("get offsets failed:{}", e.getMessage());
                e.printStackTrace();
            }
            log.info("offset value:{}", offset);
            try {
                var keyStr = "offset_" + Base64.getEncoder().encodeToString(entry.getKey().array());
                var val = Base64.getEncoder().encodeToString(entry.getValue().array());
                store.set(keyStr, val).join();
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        }
        var f = new CompletableFuture<Void>();
        f.complete(null);
        callback.onCompletion(null, null);
        offsets.set(newOffsets);
        return f;
    }

    static public List<JsonNode> getOffsets() {
        return offsets.get();
    }

    @Override
    public void configure(WorkerConfig config) {}
}
