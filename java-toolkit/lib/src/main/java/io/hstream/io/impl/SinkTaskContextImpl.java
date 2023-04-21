package io.hstream.io.impl;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.hstream.Consumer;
import io.hstream.HRecord;
import io.hstream.HStreamClient;
import io.hstream.Subscription;
import io.hstream.io.KvStore;
import io.hstream.io.ReportMessage;
import io.hstream.io.SinkRecord;
import io.hstream.io.SinkTaskContext;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.BiConsumer;
import java.util.stream.Collectors;

import lombok.extern.slf4j.Slf4j;

@Slf4j
public class SinkTaskContextImpl implements SinkTaskContext {
    static ObjectMapper mapper = new ObjectMapper();
    HRecord cfg;
    HStreamClient client;
    Consumer consumer;
    KvStore kv;
    String subId;
    AtomicInteger deliveredRecords = new AtomicInteger(0);
    AtomicInteger deliveredBytes = new AtomicInteger(0);

    @Override
    public KvStore getKvStore() {
        return kv;
    }

    @Override
    public ReportMessage getReportMessage() {
        return ReportMessage.builder()
                .deliveredRecords(deliveredBytes.getAndSet(0))
                .deliveredBytes(deliveredBytes.getAndSet(0))
                .offsets(getSubscriptionOffsets())
                .build();
    }

    List<JsonNode> getSubscriptionOffsets() {
        if (subId == null) {
            return List.of();
        }
        return client.getSubscription(subId).getOffsets()
                .stream()
                .map(offset -> mapper.createObjectNode()
                        .put("shardId", offset.getShardId())
                        .put("batchId", offset.getBatchId()))
                .collect(Collectors.toList());
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
        var taskId = cfg.getString("task");
        var cCfg = cfg.getHRecord("connector");
        var stream = cCfg.getString("stream");
        subId = kv.get("hstream_subscription_id").join();
        if (subId == null) {
            subId = "connector_sub_" + taskId;
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
                .rawRecordReceiver(((receivedRawRecord, responder) -> {
                    log.debug("received raw record:{}", receivedRawRecord.getRecordId());
                    var hRecord = tryConvertToHRecord(receivedRawRecord.getRawRecord());
                    if (hRecord != null) {
                        handler.accept(stream, List.of(makeSinkRecord(hRecord)));
                        responder.ack();
                    }
                    deliveredRecords.incrementAndGet();
                    deliveredBytes.addAndGet(receivedRawRecord.getRawRecord().length);
                }))
                .hRecordReceiver((receivedHRecord, responder) -> {
                    log.debug("received:{}", receivedHRecord.getHRecord().toJsonString());
                    handler.accept(stream, List.of(makeSinkRecord(receivedHRecord.getHRecord())));
                    responder.ack();
                    deliveredRecords.incrementAndGet();
                    deliveredBytes.addAndGet(receivedHRecord.getHRecord().getDelegate().getSerializedSize());
                })
                .build();
        consumer.startAsync().awaitRunning();
        consumer.awaitTerminated();
    }

    HRecord tryConvertToHRecord(byte[] rawRecord) {
        try {
            return HRecord.newBuilder().merge(new String(rawRecord)).build();
        } catch (Exception e) {
            return null;
        }
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
