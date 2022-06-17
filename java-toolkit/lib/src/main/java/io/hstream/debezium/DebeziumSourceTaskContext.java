package io.hstream.debezium;

import io.hstream.BufferedProducer;
import io.hstream.FileKvStore;
import io.hstream.HRecord;
import io.hstream.HStreamClient;
import io.hstream.KvStore;
import io.hstream.SourceRecord;
import io.hstream.SourceTaskContext;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;

public class DebeziumSourceTaskContext implements SourceTaskContext {
    HStreamClient client;
    Map<String, BufferedProducer> producers = new HashMap<>();
    KvStore kvStore;

    @Override
    public void init(HRecord cfg) {
        client = HStreamClient.builder().serviceUrl(cfg.getString("serviceUrl")).build();
        kvStore = new FileKvStore(cfg.getString("kvStorePath"));
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
    public void sendSync(List<SourceRecord> sourceRecordList) {
        var fs = new LinkedList<CompletableFuture<String>>();
        for (var sr : sourceRecordList) {
            fs.add(send(sr));
        }
        fs.forEach(CompletableFuture::join);
    }

    @Override
    public KvStore getKvStore() {
        return kvStore;
    }

    @Override
    public void close() throws Exception {
        producers.values().forEach(BufferedProducer::close);
        client.close();
    }
}
