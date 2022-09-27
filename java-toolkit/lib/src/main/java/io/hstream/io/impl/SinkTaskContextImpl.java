package io.hstream.io.impl;

import io.hstream.Consumer;
import io.hstream.HRecord;
import io.hstream.HStreamClient;
import io.hstream.Subscription;
import io.hstream.io.KvStore;
import io.hstream.io.SinkRecord;
import io.hstream.io.SinkTaskContext;
import java.util.List;
import java.util.UUID;
import java.util.function.BiConsumer;

public class SinkTaskContextImpl implements SinkTaskContext {
    HRecord cfg;
    HStreamClient client;
    Consumer consumer;
    KvStore kv;

    @Override
    public KvStore getKvStore() {
        return kv;
    }

    @Override
    public void init(HRecord config, KvStore kv) {
        this.cfg = config;
        this.kv = kv;
    }

    private SinkRecord makeSinkRecord(HRecord record) {
        return new SinkRecord(record);
    }

    @Override
    public void handle(BiConsumer<String, List<SinkRecord>> handler) {
        var hsCfg = cfg.getHRecord("hstream");
        client = HStreamClient.builder().serviceUrl(hsCfg.getString("serviceUrl")).build();
        var cCfg = cfg.getHRecord("connector");
        var stream = cCfg.getString("stream");
        var subId = kv.get("hstream_subscription_id").join();
        if (subId == null) {
            subId = "connector_sub_" + UUID.randomUUID();
            var sub = Subscription.newBuilder().
                    stream(stream)
                    .subscription(subId)
                    .offset(Subscription.SubscriptionOffset.EARLIEST)
                    .build();
            client.createSubscription(sub);
            kv.set("hstream_subscription_id", subId).join();
        }
        this.consumer = client.newConsumer()
                .subscription(subId)
                .hRecordReceiver((receivedHRecord, responder) -> {
                    System.out.println("received:" + receivedHRecord.getHRecord().toJsonString());
                    handler.accept(stream, List.of(makeSinkRecord(receivedHRecord.getHRecord())));
                    responder.ack();
                })
                .build();
        consumer.startAsync().awaitRunning();
        consumer.awaitTerminated();
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
