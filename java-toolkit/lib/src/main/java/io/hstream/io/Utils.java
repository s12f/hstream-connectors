package io.hstream.io;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.Objects;

public class Utils {
    public static String getSpec(Task task, String specPath) {
        try {
            var data = Objects.requireNonNull(task.getClass().getResourceAsStream(specPath)).readAllBytes();
            return new String(data, StandardCharsets.UTF_8);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }
}
