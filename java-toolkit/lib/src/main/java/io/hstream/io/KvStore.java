package io.hstream.io;

import java.util.Map;

public interface KvStore {
    void set(String key, String val);
    String get(String key);
    Map<String, String> toMap();
    void close() throws InterruptedException;
}
