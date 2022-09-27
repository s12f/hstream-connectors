package io.hstream.io;

import io.hstream.HRecord;
import java.util.List;

public interface SinkTask extends Task {
    void run(HRecord config, SinkTaskContext ctx);
}
