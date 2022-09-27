package io.hstream.io;

import io.hstream.HRecord;

public interface TaskContext {
    void init(HRecord cfg, KvStore kvStore);
    KvStore getKvStore();
    void close();
}
