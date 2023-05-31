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
import java.util.stream.Collectors;

import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import org.bson.Document;

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
                .offsets(List.of())
                .build();
    }

    @Override
    public void init(HRecord config, KvStore kv) {
        this.cfg = config;
        this.kv = kv;
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
                .batchReceiver((records, batchAckResponder) -> {
                    var sinkRecords = records.stream().map(this::makeSinkRecord).collect(Collectors.toList());
                    handleWithRetry(handler, stream, sinkRecords, batchAckResponder);
                })
                .build();
        consumer.startAsync().awaitRunning();
        latch.await();
        log.info("connector failed");
        consumer.stopAsync().awaitTerminated();
    }

    @SneakyThrows
    void handleWithRetry(BiConsumer<String, List<SinkRecord>> handler, String stream, List<SinkRecord> sinkRecords, BatchAckResponder responder) {
        int count = 0;
        int maxRetries = 3;
        int retryInterval = 5;
        while (count < maxRetries) {
            try {
                handler.accept(stream, sinkRecords);
                responder.ackAll();
                updateMetrics(sinkRecords);
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

    void updateMetrics(List<SinkRecord> records) {
        var bytesSize = 0;
        for (var r : records) {
            bytesSize += r.getRecord().getDelegate().getSerializedSize();
        }
        deliveredBytes.addAndGet(bytesSize);
        deliveredRecords.addAndGet(records.size());
    }

    SinkRecord makeSinkRecord(ReceivedRecord receivedRecord) {
        var record = receivedRecord.getRecord();
        if (record.isRawRecord()) {
            var hRecord = tryConvertToHRecord(record.getRawRecord());
            if (hRecord != null) {
                return new SinkRecord(tryFormatHRecord(hRecord));
            } else {
                log.info("invalid record, stopping task");
                latch.countDown();
                throw new RuntimeException("invalid record");
            }
        } else {
            return new SinkRecord(tryFormatHRecord(record.getHRecord()));
        }
    }

    HRecord tryConvertToHRecord(byte[] rawRecord) {
        try {
            return HRecord.newBuilder().merge(new String(rawRecord)).build();
        } catch (Exception e) {
            return null;
        }
    }

    HRecord tryFormatHRecord(HRecord hRecord) {
        var jsonStr = hRecord.toCompactJsonString();
        try {
            var doc = Document.parse(jsonStr);
            return HRecord.newBuilder().merge(doc.toJson()).build();
        } catch (Exception e) {
            return hRecord;
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
