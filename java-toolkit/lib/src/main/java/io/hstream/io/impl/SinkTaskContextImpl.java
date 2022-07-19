package io.hstream.io.impl;

import com.google.common.util.concurrent.Service;
import io.hstream.Consumer;
import io.hstream.HRecord;
import io.hstream.HRecordReceiver;
import io.hstream.HStreamClient;
import io.hstream.Subscription;
import io.hstream.io.KvStore;
import io.hstream.io.SinkRecord;
import io.hstream.io.SinkTask;
import io.hstream.io.SinkTaskContext;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.stream.Collectors;
import org.apache.commons.math3.util.Pair;

public class SinkTaskContextImpl implements SinkTaskContext {
    HRecord cfg;
    SinkTask sinkTask;
    HStreamClient client;
    List<Consumer> consumers = new ArrayList<>();
    KvStore kv;

    @Override
    public KvStore getKvStore() {
        return kv;
    }

    @Override
    public void init(HRecord config, SinkTask sinkTask) {
        this.cfg = config;
        this.sinkTask = sinkTask;
        var hsCfg = config.getHRecord("hstream");
        client = HStreamClient.builder().serviceUrl(hsCfg.getString("serviceUrl")).build();
        var streams = Utils.parseStreams(config.getHRecord("connector").getString("streams"));
        for (var entry : streams.entrySet()) {
            var subId = "connector_sub_" + UUID.randomUUID();
            var sub = Subscription.newBuilder().stream(entry.getKey()).subscription(subId).build();
            client.createSubscription(sub);
            consumers.add(client.newConsumer()
                    .subscription(subId)
                    .hRecordReceiver((receivedHRecord, responder) -> {
                        System.out.println("received:" + receivedHRecord.getHRecord().toJsonString());
                        sinkTask.send(entry.getValue(), List.of(makeSinkRecord(receivedHRecord.getHRecord())));
                    })
                    .build());
        }
    }

    private SinkRecord makeSinkRecord(HRecord record) {
        return new SinkRecord(record);
    }

    @Override
    public void run() {
        consumers.forEach(c -> c.startAsync().awaitRunning());
    }

    @Override
    public void close() {
        consumers.forEach(Service::stopAsync);
        try {
            client.close();
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }
}
