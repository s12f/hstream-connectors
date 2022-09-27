package io.hstream.io.impl;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.hstream.io.KvStore;
import io.hstream.io.internal.Channel;
import java.util.concurrent.CompletableFuture;

public class ChannelKvStore implements KvStore {
    Channel channel;

    public ChannelKvStore(Channel channel) {
        this.channel = channel;
    }

    @Override
    public CompletableFuture<Void> set(String key, String val) {
        var kvMsg = new ObjectMapper().createObjectNode()
                .put("type", "kv")
                .put("action", "set")
                .put("key", key)
                .put("value", val);
        return channel.call(kvMsg).thenApply(c -> null);
    }

    @Override
    public CompletableFuture<String> get(String key) {
        var kvMsg = new ObjectMapper().createObjectNode()
                .put("type", "kv")
                .put("action", "get")
                .put("key", key);
        return channel.call(kvMsg).thenApply(n -> n.isNull() ? null : n.asText());
    }

//    @Override
//    public CompletableFuture<Map<String, String>> toMap() {
//        var mapper = new ObjectMapper();
//        var kvMsg = mapper.createObjectNode()
//                .put("type", "kv")
//                .put("action", "toMap");
//        return channel.call(kvMsg).thenApply(j -> mapper.convertValue(j, new TypeReference<>() {}));
//    }

    @Override
    public void close() throws InterruptedException {
        channel.close();
    }
}
