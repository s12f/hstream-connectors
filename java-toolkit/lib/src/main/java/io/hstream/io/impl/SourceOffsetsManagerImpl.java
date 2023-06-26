package io.hstream.io.impl;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.hstream.io.KvStore;
import io.hstream.io.SourceOffsetsManager;
import lombok.SneakyThrows;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

public class SourceOffsetsManagerImpl implements SourceOffsetsManager {
    KvStore kvStore;
    String offsetsKey;
    ConcurrentHashMap<String, String> offsets = new ConcurrentHashMap<>();
    AtomicReference<HashMap<String, String>> storedOffsets = new AtomicReference<>(new HashMap<>());
    ScheduledExecutorService executor = Executors.newSingleThreadScheduledExecutor();
    static ObjectMapper mapper = new ObjectMapper();
    AtomicInteger bufferState = new AtomicInteger(0);

    public SourceOffsetsManagerImpl(KvStore kvStore, String prefix) {
        this.kvStore = kvStore;
        this.offsetsKey = prefix + "_offsets";
        init();
        executor.scheduleAtFixedRate(this::storeOffsets, 1, 1, TimeUnit.SECONDS);
    }

    @SneakyThrows
    void init() {
        var offsetsStr = kvStore.get(offsetsKey).get();
        if (offsetsStr != null && !offsetsStr.isEmpty()) {
            var stored = mapper.readValue(offsetsStr, new TypeReference<HashMap<String, String>>(){});
            storedOffsets.set(stored);
            offsets.putAll(stored);
        }
    }

    @Override
    public void update(String shardId, String recordId) {
        offsets.put(shardId, recordId);
        bufferState.set(0);
    }

    @SneakyThrows
    void storeOffsets() {
        if (bufferState.get() == 2) {
            return;
        }
        var stored = new HashMap<>(offsets);
        kvStore.set(offsetsKey, mapper.writeValueAsString(stored));
        storedOffsets.set(stored);
        bufferState.incrementAndGet();
    }

    @Override
    public Map<String, String> getStoredOffsets() {
        return new HashMap<>(storedOffsets.get());
    }

    @Override
    public void close() {
        executor.shutdown();
    }
}
