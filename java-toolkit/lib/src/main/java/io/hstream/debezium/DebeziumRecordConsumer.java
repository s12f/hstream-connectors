package io.hstream.debezium;

import io.debezium.engine.ChangeEvent;
import io.debezium.engine.DebeziumEngine;
import io.hstream.HRecord;
import io.hstream.Record;
import io.hstream.SourceRecord;
import io.hstream.SourceTaskContext;
import java.util.List;

public class DebeziumRecordConsumer implements DebeziumEngine.ChangeConsumer<ChangeEvent<String, String>> {
    SourceTaskContext ctx;

    public DebeziumRecordConsumer(SourceTaskContext ctx) {
        this.ctx = ctx;
    }

    @Override
    public void handleBatch(List<ChangeEvent<String, String>> records, DebeziumEngine.RecordCommitter<ChangeEvent<String, String>> committer) throws InterruptedException {
        final int[] rs = {records.size()};
        for (var r : records) {
            try {
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
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        }
    }

    private SourceRecord toSourceRecord(ChangeEvent<String, String> record) throws Exception {
        System.out.println("key:" + record.key());
        System.out.println("val:" + record.value());
        System.out.println("dst:" + record.destination());
        System.out.println();
        var hRecord = HRecord.newBuilder().merge(record.value()).build();
        System.out.println("hrecord:" + hRecord.toJsonString());
        var source = hRecord.getHRecord("source");
        source.getString("file");
        var r = Record.newBuilder().hRecord(hRecord).build();
        return new SourceRecord(record.destination(), r);
    }
}
