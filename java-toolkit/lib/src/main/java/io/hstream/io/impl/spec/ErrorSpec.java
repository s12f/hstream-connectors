package io.hstream.io.impl.spec;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.IntNode;
import lombok.Getter;

import java.util.List;

public class ErrorSpec {
    static List<SpecProperty> properties() {
        return List.of(new SkipCount(), new Stream(), new MaxRetries());
    }

    static public String SKIP_COUNT_NAME = "task.error.skipCount";
    static public String STREAM_NAME = "task.error.stream";
    static public String MAX_RETRIES = "task.error.maxRetries";

    @Getter
    static public class SkipCount implements SpecProperty {
        String name = SKIP_COUNT_NAME;
        String uiShowName = "skip error count";
        String type = "string";
        JsonNode defaultValue = new IntNode(-1);
    }

    @Getter
    static public class Stream implements SpecProperty {
        String name = STREAM_NAME;
        String uiShowName = "Error Stream";
        String type = "string";
    }

    @Getter
    static public class MaxRetries implements SpecProperty {
        String name = MAX_RETRIES;
        String uiShowName = "Error Max Retries";
        String type = "string";
        JsonNode defaultValue = new IntNode(3);
    }
}
