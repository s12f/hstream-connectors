package io.hstream.io;

import java.io.IOException;

public interface KvStore {
    void set(String key, byte[] val) throws Exception;
    byte[] get(String key) throws Exception;
    void close() throws InterruptedException;
}
