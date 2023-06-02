package io.hstream.io.impl;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.hstream.io.KvStore;
import io.hstream.io.SinkOffsetsManager;
import lombok.SneakyThrows;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

public class SinkOffsetsManagerImpl implements SinkOffsetsManager {
    KvStore kvStore;
    String offsetsKey;
    ConcurrentHashMap<Long, String> offsets = new ConcurrentHashMap<>();
    AtomicReference<HashMap<Long, String>> storedOffsets = new AtomicReference<>(new HashMap<>());
    ScheduledExecutorService executor = Executors.newSingleThreadScheduledExecutor();
    static ObjectMapper mapper = new ObjectMapper();

    SinkOffsetsManagerImpl(KvStore kvStore, String prefix) {
        this.kvStore = kvStore;
        this.offsetsKey = prefix + "_offsets";
        init();
        executor.scheduleAtFixedRate(this::storeOffsets, 1, 1, TimeUnit.SECONDS);
    }

    @SneakyThrows
    void init() {
        var offsetsStr = kvStore.get(offsetsKey).get();
        if (offsetsStr != null && !offsetsStr.isEmpty()) {
            var stored = mapper.readValue(offsetsStr, new TypeReference<HashMap<Long, String>>(){});
            storedOffsets.set(stored);
            offsets.putAll(stored);
        }
    }

    @Override
    public void update(long shardId, String recordId) {
        offsets.compute(shardId, (k, v) -> recordId);
    }

    @SneakyThrows
    void storeOffsets() {
        var stored = new HashMap<>(offsets);
        kvStore.set(offsetsKey + "_offsets", mapper.writeValueAsString(stored));
        storedOffsets.set(stored);
    }

    @Override
    public Map<Long, String> getStoredOffsets() {
        return new HashMap<>(storedOffsets.get());
    }

    @Override
    public void close() {
        executor.shutdown();
    }
}
