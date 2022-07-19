package io.hstream.io;

import io.hstream.HRecord;
import java.util.List;

public interface SinkTaskContext extends TaskContext {
    void init(HRecord config, SinkTask sinkTask);
    void run();
}
