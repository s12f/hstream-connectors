package io.hstream.io;

import io.hstream.HRecord;

public interface Task {
    String spec();
    default void check(HRecord config) {}
    void run();
    void stop() throws Exception;
}
