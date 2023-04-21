package io.hstream.io;

import com.fasterxml.jackson.databind.JsonNode;
import io.hstream.HRecord;

public interface Task {
    JsonNode spec();
    default CheckResult check(HRecord config) {
        return CheckResult.builder()
                .result(true)
                .build();
    }
    void stop();
}
