package io.hstream.io.impl;

import io.hstream.io.KvStore;
import io.hstream.io.Rpc;
import java.util.concurrent.CompletableFuture;

public class ChannelKvStore implements KvStore {
    Rpc rpc;

    public ChannelKvStore(Rpc rpc) {
        this.rpc = rpc;
    }

    @Override
    public CompletableFuture<Void> set(String key, String val) {
        return rpc.kvSet(key, val);
    }

    @Override
    public CompletableFuture<String> get(String key) {
        return rpc.kvGet(key);
    }

    @Override
    public void close() {}
}
