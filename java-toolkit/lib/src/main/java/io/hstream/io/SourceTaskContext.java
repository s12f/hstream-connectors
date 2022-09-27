package io.hstream.io;

import io.hstream.HRecord;
import java.util.concurrent.CompletableFuture;

public interface SourceTaskContext extends TaskContext {
    CompletableFuture<String> send(SourceRecord sourceRecord);
}
