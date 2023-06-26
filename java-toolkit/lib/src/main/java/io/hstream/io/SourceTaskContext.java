package io.hstream.io;

import java.util.concurrent.CompletableFuture;

public interface SourceTaskContext extends TaskContext {
    CompletableFuture<String> send(SourceRecord sourceRecord);
//    SourceOffsetsManager getOffsetsManager();
}
