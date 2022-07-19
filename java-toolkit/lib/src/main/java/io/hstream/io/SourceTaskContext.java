package io.hstream.io;

import io.hstream.HRecord;
import io.hstream.io.SourceRecord;
import java.util.List;
import java.util.concurrent.CompletableFuture;

public interface SourceTaskContext extends TaskContext {
    void init(HRecord cfg);
    CompletableFuture<String> send(SourceRecord sourceRecord);
}
