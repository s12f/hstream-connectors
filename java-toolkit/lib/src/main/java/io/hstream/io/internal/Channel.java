package io.hstream.io.internal;

import com.fasterxml.jackson.databind.JsonNode;
import io.hstream.HRecord;
import java.util.concurrent.CompletableFuture;
import java.util.function.Consumer;

public interface Channel {
    // send a message to IO Runtime
    void send(JsonNode msg);
    // send a message to IO Runtime and receive the response
    CompletableFuture<JsonNode> call(JsonNode msg);
    // handle messages from IO Runtime
    void handle(Consumer<JsonNode> handler);
    void close();
}
