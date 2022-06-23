package source.debezium;

import io.hstream.io.KvStore;
import java.nio.ByteBuffer;
import java.util.Base64;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Future;
import org.apache.kafka.connect.runtime.WorkerConfig;
import org.apache.kafka.connect.util.Callback;

// for debezium
public class OffsetBackingStore implements org.apache.kafka.connect.storage.OffsetBackingStore {
    static KvStore store;
    static public void setKvStore(KvStore kvStore) {
        store = kvStore;
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
                var keyStr = Base64.getEncoder().encodeToString(key.array());
                var val = store.get(keyStr);
                if (val != null) {
                    res.put(key, ByteBuffer.wrap(val));
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
            try {
                var keyStr = Base64.getEncoder().encodeToString(entry.getKey().array());
                var val = entry.getValue().array();
                store.set(keyStr, val);
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        }
        var f = new CompletableFuture<Void>();
        f.complete(null);
        callback.onCompletion(null, null);
        return f;
    }

    @Override
    public void configure(WorkerConfig config) {}
}
