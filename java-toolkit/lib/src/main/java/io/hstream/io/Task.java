package io.hstream.io;

import com.fasterxml.jackson.databind.JsonNode;
import io.hstream.HRecord;

public interface Task {
    JsonNode spec();
    default void check(HRecord config) {}
    void stop();
}
