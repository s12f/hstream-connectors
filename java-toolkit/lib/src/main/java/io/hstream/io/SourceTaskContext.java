package io.hstream.io;

import io.hstream.HRecord;
import io.hstream.io.SourceRecord;
import java.util.List;
import java.util.concurrent.CompletableFuture;

public interface SourceTaskContext extends TaskContext {
    CompletableFuture<String> send(SourceRecord sourceRecord);
    void sendSync(List<SourceRecord> sourceRecordList);
}
