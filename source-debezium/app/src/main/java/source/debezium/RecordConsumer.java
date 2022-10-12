package source.debezium;

import io.debezium.engine.ChangeEvent;
import io.debezium.engine.DebeziumEngine;
import io.hstream.HRecord;
import io.hstream.Record;
import io.hstream.io.SourceRecord;
import io.hstream.io.SourceTaskContext;
import java.util.List;
import java.util.function.Function;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class RecordConsumer implements DebeziumEngine.ChangeConsumer<ChangeEvent<String, String>> {
    SourceTaskContext ctx;
    String serverName;
    String stream;
    Function<HRecord, HRecord> keyMapper;

    public RecordConsumer(SourceTaskContext ctx, String serverName, String stream, Function<HRecord, HRecord> keyMapper) {
        this.ctx = ctx;
        this.serverName = serverName;
        this.stream = stream;
        this.keyMapper = keyMapper;
    }

    @Override
    public void handleBatch(List<ChangeEvent<String, String>> records, DebeziumEngine.RecordCommitter<ChangeEvent<String, String>> committer) throws InterruptedException {
        final int[] rs = {records.size()};
        for (var r : records) {
            log.info("key:{}", r.key());
            log.info("val:{}", r.value());
            log.info("dst:{}", r.destination());
            // ignore ddl events
            if (r.destination().equals(serverName)) {
                continue;
            }
            if (r.key() == null) {
                continue;
            }
            var future = ctx.send(toSourceRecord(r));
            future.whenCompleteAsync((x, y) -> {
                try {
                    committer.markProcessed(r);
                    rs[0]--;
                    if (rs[0] == 0) {
                        committer.markBatchFinished();
                    }
                } catch (InterruptedException e) {
                    throw new RuntimeException(e);
                }
            });
        }
    }

    private SourceRecord toSourceRecord(ChangeEvent<String, String> record) {
        var keyRecord = HRecord.newBuilder().merge(record.key()).build();
        if (keyMapper != null) {
            keyRecord = keyMapper.apply(keyRecord);
        }
        var hRecordBuilder
                = HRecord.newBuilder()
                .put("key", keyRecord);
        if (record.value() == null) {
            hRecordBuilder.putNull("value");
        } else {
            hRecordBuilder.put("value", HRecord.newBuilder().merge(record.value()).build());
        }
        var hRecord = hRecordBuilder.build();
        log.info("hRecord:{}", hRecord.toCompactJsonString());
        var r = Record.newBuilder().hRecord(hRecord).build();
        return new SourceRecord(stream, r);
    }
}
