package io.hstream.io;

import java.util.List;
import java.util.function.BiConsumer;

public interface SinkTaskContext extends TaskContext {
    void handle(BiConsumer<String, List<SinkRecord>> handler);
}
