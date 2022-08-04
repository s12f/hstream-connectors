package io.hstream.io.impl;

import com.google.common.util.concurrent.Service;
import io.hstream.Consumer;
import io.hstream.HRecord;
import io.hstream.HStreamClient;
import io.hstream.Subscription;
import io.hstream.io.KvStore;
import io.hstream.io.SinkRecord;
import io.hstream.io.SinkTask;
import io.hstream.io.SinkTaskContext;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

public class SinkTaskContextImpl implements SinkTaskContext {
    HRecord cfg;
    SinkTask sinkTask;
    HStreamClient client;
    Consumer consumer;
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
        kv = Utils.makeKvStoreFromConfig(cfg);
        var cCfg = cfg.getHRecord("connector");
        var stream = cCfg.getString("stream");
        var subId = kv.get("hstream_subscription_id");
        if (subId == null) {
            subId = "connector_sub_" + UUID.randomUUID();
            var sub = Subscription.newBuilder().
                    stream(stream)
                    .subscription(subId)
                    .offset(Subscription.SubscriptionOffset.EARLIEST)
                    .build();
            client.createSubscription(sub);
            kv.set("hstream_subscription_id", subId);
        }
        consumer = client.newConsumer()
                .subscription(subId)
                .hRecordReceiver((receivedHRecord, responder) -> {
                    System.out.println("received:" + receivedHRecord.getHRecord().toJsonString());
                    sinkTask.send(stream, List.of(makeSinkRecord(receivedHRecord.getHRecord())));
                    responder.ack();
                })
                .build();
    }

    private SinkRecord makeSinkRecord(HRecord record) {
        return new SinkRecord(record);
    }

    @Override
    public void run() {
        consumer.startAsync().awaitRunning();
    }

    @Override
    public void close() {
        try {
            consumer.stopAsync().awaitTerminated();
            client.close();
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }
}
