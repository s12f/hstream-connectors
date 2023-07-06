package io.hstream.io.test;

import io.hstream.HRecord;
import io.hstream.io.KvStore;
import io.hstream.io.ReportMessage;
import io.hstream.io.SinkRecord;
import io.hstream.io.SinkTaskContext;
import lombok.SneakyThrows;

import java.util.List;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.function.BiConsumer;

public class FakeSinkTaskContext implements SinkTaskContext {
    final LinkedBlockingQueue<List<SinkRecord>> data = new LinkedBlockingQueue<>(10);
    KvStore kvStore;

    @SneakyThrows
    @Override
    public void handle(BiConsumer<String, List<SinkRecord>> handler) {
        while (true) {
            handler.accept("stream01", data.take());
        }
    }

    @SneakyThrows
    public void appendRecords(List<SinkRecord> records) {
        data.put(records);
    }

    @Override
    public void init(HRecord cfg, KvStore kvStore) {
        this.kvStore = kvStore;
    }

    @Override
    public KvStore getKvStore() {
        return kvStore;
    }

    @Override
    public ReportMessage getReportMessage() {
        return null;
    }

    @Override
    public void close() {}
}
