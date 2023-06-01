package io.hstream.io;

import java.util.Map;

public interface SinkOffsetsManager {
    void update(long shardId, String recordId);
    Map<Long, String> getStoredOffsets();
    void close();
}
