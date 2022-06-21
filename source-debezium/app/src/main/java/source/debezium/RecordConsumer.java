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
            if (r.value() == null) {
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
        System.out.println("key:" + record.key());
        System.out.println("val:" + record.value());
        System.out.println("dst:" + record.destination());
        System.out.println();
        var hRecord = HRecord.newBuilder().merge(record.value()).build();
        System.out.println("hrecord:" + hRecord.toJsonString());
        var r = Record.newBuilder().hRecord(hRecord).build();
        return new SourceRecord(record.destination(), r);
    }
}
