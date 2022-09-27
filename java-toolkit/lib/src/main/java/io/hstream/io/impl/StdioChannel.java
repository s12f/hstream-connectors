package io.hstream.io.impl;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.hstream.io.internal.Channel;
import java.util.HashMap;
import java.util.Map;
import java.util.Scanner;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Consumer;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class StdioChannel implements Channel {
    String CONNECTOR_CALL = "connector_call";
    String CONNECTOR_SEND = "connector_send";
    final Map<String, CompletableFuture<JsonNode>> futures = new HashMap<>();
    AtomicBoolean closed = new AtomicBoolean(false);
    int maxResponseTimeout = 10;

    @Override
    public void send(JsonNode msg) {
        var msgId = UUID.randomUUID().toString();
        log.info("send message, id:{}, msg:{}", msgId, msg.toString());
        writeMsg(CONNECTOR_SEND, msgId, msg);
    }

    @Override
    public CompletableFuture<JsonNode> call(JsonNode msg) {
        var msgId = UUID.randomUUID().toString();
        log.info("call message, id:{}, msg:{}", msgId, msg.toString());
        var future = new CompletableFuture<JsonNode>();
        future.orTimeout(maxResponseTimeout, TimeUnit.SECONDS);
        synchronized (futures) {
            futures.put(msgId, future);
            writeMsg(CONNECTOR_CALL, msgId, msg);
        }
        return future;
    }

    @Override
    public void handle(Consumer<JsonNode> handler) {
        Scanner scanner = new Scanner(System.in);
        try {
            while (true) {
                Thread.sleep(100);
                if (closed.get()) {
                    return;
                }
                if (!scanner.hasNext()) {
                    continue;
                }
                var line = scanner.nextLine();
                log.info("received line:{}", line);
                try {
                    var msg = new ObjectMapper().readTree(line);
                    var msgType = msg.get("type").asText();
                    if (msgType.equals(CONNECTOR_CALL)) {
                        var msgId = msg.get("id").asText();
                        synchronized (futures) {
                            var future = futures.get(msgId);
                            if (future == null) {
                                log.warn("future not found:{}", msgId);
                                continue;
                            }
                            future.complete(msg.get("message"));
                            futures.remove(msgId);
                        }
                    } else {
                        handler.accept(msg);
                    }
                } catch (Exception e) {
                    log.warn("received an invalid cmd:{}", line);
                    log.info("error:{}", e.getMessage());
                }
            }
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }
    }

    void writeMsg(String type, String msgId, JsonNode msg) {
        var channelMessage = new ObjectMapper().createObjectNode()
                .put("type", type)
                .put("id", msgId)
                .set("message", msg);
        System.out.println(channelMessage.toString());
        System.out.flush();
    }

    @Override
    public void close() {
        log.info("closing stdio channel");
        closed.set(true);
        try {
            Thread.sleep(100);
        } catch (InterruptedException ignored) {
        }
        synchronized (futures) {
            futures.values().forEach(f -> f.completeExceptionally(new RuntimeException("channel have been closed")));
        }
    }
}
