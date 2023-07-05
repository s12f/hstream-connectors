package io.hstream.io.test;

import io.hstream.io.KvStore;

import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;

public class FakeKvStore implements KvStore {
    Map<String, String> data = new ConcurrentHashMap<>();

    @Override
    public CompletableFuture<Void> set(String key, String val) {
        data.put(key, val);
        var future = new CompletableFuture<Void>();
        future.complete(null);
        return future;
    }

    @Override
    public CompletableFuture<String> get(String key) {
        var future = new CompletableFuture<String>();
        future.complete(data.get(key));
        return future;
    }

    @Override
    public void close() throws InterruptedException {}
}
