package io.hstream.io;

import java.util.Map;
import java.util.concurrent.CompletableFuture;

public interface KvStore {
    CompletableFuture<Void> set(String key, String val);
    CompletableFuture<String> get(String key);
//    CompletableFuture<Map<String, String>> toMap();
    void close() throws InterruptedException;
}
