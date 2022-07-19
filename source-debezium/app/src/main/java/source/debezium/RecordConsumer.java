package source.debezium;

import io.debezium.engine.ChangeEvent;
import io.debezium.engine.DebeziumEngine;
import io.hstream.HRecord;
import io.hstream.Record;
import io.hstream.io.SourceRecord;
import io.hstream.io.SourceTaskContext;
import java.util.List;

public class RecordConsumer implements DebeziumEngine.ChangeConsumer<ChangeEvent<String, String>> {
    SourceTaskContext ctx;

    public RecordConsumer(SourceTaskContext ctx) {
        this.ctx = ctx;
    }

    @Override
    public void handleBatch(List<ChangeEvent<String, String>> records, DebeziumEngine.RecordCommitter<ChangeEvent<String, String>> committer) throws InterruptedException {
        final int[] rs = {records.size()};
        for (var r : records) {
            System.out.println("key:" + r.key());
            System.out.println("val:" + r.value());
            System.out.println("dst:" + r.destination());
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
        var hRecordBuilder
                = HRecord.newBuilder()
                .put("key", keyRecord);
        if (record.value() == null) {
            hRecordBuilder.putNull("value");
        } else {
            hRecordBuilder.put("value", HRecord.newBuilder().merge(record.value()).build());
        }
        var hRecord = hRecordBuilder.build();
        System.out.println("hRecord:" + hRecord.toJsonString());
        var r = Record.newBuilder().hRecord(hRecord).build();
        return new SourceRecord(record.destination(), r);
    }
}
