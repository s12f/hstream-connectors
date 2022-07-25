package io.hstream.io;

import io.hstream.HRecord;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.Objects;

public interface Task {
    String spec();
    default void check(HRecord config) {}
    void stop();
}
