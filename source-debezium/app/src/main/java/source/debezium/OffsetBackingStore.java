package source.debezium;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.hstream.io.KvStore;
import java.nio.ByteBuffer;
import java.util.*;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Future;

import io.hstream.io.SourceOffsetsManager;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.connect.json.JsonDeserializer;
import org.apache.kafka.connect.json.JsonSerializer;
import org.apache.kafka.connect.runtime.WorkerConfig;
import org.apache.kafka.connect.util.Callback;

@Slf4j
public class OffsetBackingStore implements org.apache.kafka.connect.storage.OffsetBackingStore {
    static KvStore store;
    static String namespace;
    static SourceOffsetsManager offsetsManager;
    static ObjectMapper mapper = new ObjectMapper();
    JsonDeserializer jsonDeserializer = new JsonDeserializer();
    JsonSerializer jsonSerializer = new JsonSerializer();

    static public void setKvStore(KvStore kvStore) {
        store = kvStore;
    }

    static public void setNamespace(String namespace) {
        OffsetBackingStore.namespace = namespace;
    }

    static public void setOffsetsManager(SourceOffsetsManager offsetsManager) {
        OffsetBackingStore.offsetsManager = offsetsManager;
    }

    @Override
    public void start() {}

    @Override
    public void stop() {}

    @Override
    public Future<Map<ByteBuffer, ByteBuffer>> get(Collection<ByteBuffer> keys) {
        var res = new HashMap<ByteBuffer, ByteBuffer>();
        var storedOffsets = offsetsManager.getStoredOffsets();
        for (var key : keys) {
            try {
                var keyStr = "offset_" + Base64.getEncoder().encodeToString(key.array());
                var val = storedOffsets.get(keyStr);
                if (val != null) {
                    res.put(key, ByteBuffer.wrap(jsonSerializer.serialize(namespace, mapper.readTree(val))));
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
        for (var entry : values.entrySet()) {
            JsonNode offset = jsonDeserializer.deserialize(namespace, entry.getValue().array());
            log.info("offset value:{}", offset);
            var keyStr = "offset_" + Base64.getEncoder().encodeToString(entry.getKey().array());
            var val = offset.toString();
            offsetsManager.update(keyStr, val);
        }
        var f = new CompletableFuture<Void>();
        f.complete(null);
        callback.onCompletion(null, null);
        return f;
    }

    @SneakyThrows
    static public List<JsonNode> getOffsets() {
        var offsets = new LinkedList<JsonNode>();
        for (var entry : offsetsManager.getStoredOffsets().entrySet()) {
            log.info("offset entry:{}, {}", entry.getKey(), entry.getValue());
            offsets.add(mapper.readTree(entry.getValue()));
        }
        return offsets;
    }

    @Override
    public void configure(WorkerConfig config) {}
}
