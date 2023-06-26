package io.hstream.io;

import java.util.Map;

public interface SourceOffsetsManager {
    void update(String key, String value);
    Map<String, String> getStoredOffsets();
    void close();
}
