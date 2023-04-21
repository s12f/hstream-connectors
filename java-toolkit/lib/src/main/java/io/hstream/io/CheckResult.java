package io.hstream.io;

import com.fasterxml.jackson.databind.JsonNode;
import lombok.Builder;
import lombok.Data;
import lombok.extern.jackson.Jacksonized;

@Data
@Jacksonized
@Builder
public class CheckResult {
    Boolean result;
    CheckResultType type;
    String message;
    JsonNode detail;

    public enum CheckResultType {
        CONFIG,
        CONNECTION,
        KEYS,
    }

    static public CheckResult ok() {
        return CheckResult.builder().result(true).build();
    }
}
