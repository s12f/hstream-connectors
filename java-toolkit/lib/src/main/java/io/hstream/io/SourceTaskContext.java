package io.hstream.io;

import io.hstream.HRecord;
import io.hstream.io.SourceRecord;
import java.util.List;
import java.util.concurrent.CompletableFuture;

public interface SourceTaskContext {
    void init(HRecord cfg);
    CompletableFuture<String> send(SourceRecord sourceRecord);
    void sendSync(List<SourceRecord> sourceRecordList);
    KvStore getKvStore();
    void close() throws Exception;
}
