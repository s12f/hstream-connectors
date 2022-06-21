package io.hstream.io;

import java.io.IOException;

public interface KvStore {
    void set(String key, String val) throws Exception;
    String get(String key) throws Exception;
}
