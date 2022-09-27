package io.hstream.io.impl;

import io.hstream.BufferedProducer;
import io.hstream.HRecord;
import io.hstream.HStreamClient;
import io.hstream.io.KvStore;
import io.hstream.io.SourceRecord;
import io.hstream.io.SourceTaskContext;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.CompletableFuture;

public class SourceTaskContextImpl implements SourceTaskContext {
    HStreamClient client;
    Map<String, BufferedProducer> producers = new HashMap<>();
    KvStore kvStore;

    @Override
    public void init(HRecord cfg, KvStore kv) {
        var hsCfg = cfg.getHRecord("hstream");
        this.kvStore = kv;
        client = HStreamClient.builder().serviceUrl(hsCfg.getString("serviceUrl")).build();
    }

    @Override
    public CompletableFuture<String> send(SourceRecord sourceRecord) {
        var stream = sourceRecord.stream;
        var producer = producers.get(stream);
        if (producer == null) {
            if (client.listStreams().stream().noneMatch(s -> stream.equals(s.getStreamName()))) {
                client.createStream(stream);
            }
            producer = client.newBufferedProducer().stream(sourceRecord.stream).build();
            producers.put(stream, producer);
        }
        return producer.write(sourceRecord.record);
    }

    @Override
    public KvStore getKvStore() {
        return kvStore;
    }

    @Override
    public void close() {
        producers.values().forEach(BufferedProducer::close);
        try {
            client.close();
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }
}
