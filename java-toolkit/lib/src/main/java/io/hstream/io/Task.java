package io.hstream.io;

import io.hstream.HRecord;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.Objects;

public interface Task {
    default String spec() {
        try {
            var data = Objects.requireNonNull(this.getClass().getResourceAsStream("/spec.json")).readAllBytes();
            return new String(data, StandardCharsets.UTF_8);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }
    default void check(HRecord config) {}
    void stop();
}
