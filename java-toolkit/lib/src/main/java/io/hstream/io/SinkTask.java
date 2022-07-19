package io.hstream.io;

import io.hstream.HRecord;
import java.util.List;

public interface SinkTask extends Task {
    void init(HRecord config, SinkTaskContext ctx);
    void send(String stream, List<SinkRecord> records);
}
