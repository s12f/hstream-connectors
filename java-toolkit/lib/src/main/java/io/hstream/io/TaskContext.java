package io.hstream.io;

import io.hstream.HRecord;

public interface TaskContext {
    KvStore getKvStore();
    void close();
}
