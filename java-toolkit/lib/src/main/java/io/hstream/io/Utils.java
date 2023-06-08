package io.hstream.io;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.protobuf.Struct;
import com.google.protobuf.Value;
import com.google.protobuf.util.JsonFormat;
import io.hstream.HRecord;
import lombok.SneakyThrows;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.stream.Collectors;

public class Utils {
    static ObjectMapper mapper = new ObjectMapper();
    public static void runWithTimeout(int timeout, Runnable runnable) {
        var thread = new Thread(runnable);
        thread.start();
        try {
            thread.join(timeout * 1000L);
        } catch (InterruptedException ignored) {}
    }

    public static JsonNode getSpec(Task task, String specPath) {
        try {
            return new ObjectMapper().readTree(Objects.requireNonNull(task.getClass().getResourceAsStream(specPath)));
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    public static Object pbValueToObject(Value value) {
        switch (value.getKindCase()) {
            case NULL_VALUE:
                return null;
            case NUMBER_VALUE:
                return value.getNumberValue();
            case STRING_VALUE:
                return value.getStringValue();
            case BOOL_VALUE:
                return value.getBoolValue();
            case LIST_VALUE:
                return value.getListValue().getValuesList().stream()
                        .map(Utils::pbValueToObject)
                        .collect(Collectors.toList());
            case STRUCT_VALUE:
                return pbStructToMap(value.getStructValue());
            default:
                return new RuntimeException("invalid value:" + value);
        }
    }

    @SneakyThrows
    static public Map<String, Object> pbStructToMap(Struct struct) {
        var jsonStr = JsonFormat.printer().omittingInsignificantWhitespace().print(struct);
        return mapper.readValue(jsonStr, new TypeReference<>() {});
    }

    static public Map<String, Object> hRecordToMap(HRecord hRecord) {
        return pbStructToMap(hRecord.getDelegate());
    }
}
