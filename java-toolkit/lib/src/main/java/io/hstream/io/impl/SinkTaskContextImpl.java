package io.hstream.io.impl;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.hstream.*;
import io.hstream.io.KvStore;
import io.hstream.io.ReportMessage;
import io.hstream.io.SinkRecord;
import io.hstream.io.SinkTaskContext;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.BiConsumer;

import lombok.SneakyThrows;
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
    CountDownLatch latch = new CountDownLatch(1);

    @Override
    public KvStore getKvStore() {
        return kv;
    }

    @Override
    public ReportMessage getReportMessage() {
        return ReportMessage.builder()
                .deliveredRecords(deliveredRecords.getAndSet(0))
                .deliveredBytes(deliveredBytes.getAndSet(0))
                .offsets(List.of(mapper.createObjectNode().put("sub", subId)))
                .build();
    }

    @Override
    public void init(HRecord config, KvStore kv) {
        this.cfg = config;
        this.kv = kv;
    }

    private SinkRecord makeSinkRecord(HRecord record) {
        return new SinkRecord(record);
    }

    @SneakyThrows
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
        latch = new CountDownLatch(1);
        this.consumer = client.newConsumer()
                .subscription(subId)
                .rawRecordReceiver(((receivedRawRecord, responder) -> {
                    var hRecord = tryConvertToHRecord(receivedRawRecord.getRawRecord());
                    if (hRecord != null) {
                        handleWithRetry(handler, stream, makeSinkRecord(hRecord), responder);
                    } else {
                        log.info("invalid record, stopping task");
                        latch.countDown();
                    }
                }))
                .hRecordReceiver((receivedHRecord, responder) -> {
                    handleWithRetry(handler, stream, makeSinkRecord(receivedHRecord.getHRecord()), responder);
                })
                .build();
        consumer.startAsync().awaitRunning();
        latch.await();
        log.info("connector failed");
        consumer.stopAsync().awaitTerminated();
    }

    @SneakyThrows
    void handleWithRetry(BiConsumer<String, List<SinkRecord>> handler, String stream, SinkRecord sinkRecord, Responder responder) {
        int count = 0;
        int maxRetries = 3;
        int retryInterval = 5;
        while (count < maxRetries) {
            try {
                handler.accept(stream, List.of(sinkRecord));
                responder.ack();
                deliveredRecords.incrementAndGet();
                deliveredBytes.addAndGet(sinkRecord.record.getDelegate().getSerializedSize());
                return;
            } catch (Throwable e) {
                log.warn("deliver record failed:{}", e.getMessage());
                e.printStackTrace();
                count++;
                Thread.sleep(retryInterval * count * 1000L);
            }
        }
        // failed
        latch.countDown();
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
