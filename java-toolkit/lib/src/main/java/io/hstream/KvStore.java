package io.hstream;

import java.io.IOException;

public interface KvStore {
    void set(String key, String val) throws IOException, Exception;
    String get(String key) throws Exception;
}
