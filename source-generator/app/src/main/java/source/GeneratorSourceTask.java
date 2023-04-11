package source;

import com.fasterxml.jackson.databind.JsonNode;
import io.hstream.HRecord;
import io.hstream.Record;
import io.hstream.io.*;
import io.hstream.io.impl.SourceTaskContextImpl;
import lombok.SneakyThrows;

import java.time.Instant;
import java.util.Random;

public class GeneratorSourceTask implements SourceTask {
    volatile Boolean needStop = false;
    Random rnd = new Random();
    SourceTaskContext ctx;
    KvStore kv;
    String stream;

    @SneakyThrows
    @Override
    public void run(HRecord cfg, SourceTaskContext ctx) {
        this.ctx = ctx;
        this.kv = ctx.getKvStore();
        this.stream = cfg.getString("stream");
        var batchSize = cfg.getInt("batchSize");
        var period = cfg.getInt("period");
        assert batchSize > 0;
        assert period > 0;
        var seq = 0;
        var seqStr = kv.get("sequence").get();
        if (seqStr != null && !seqStr.isEmpty()) {
            seq = Integer.parseInt(seqStr);
        }
        while (true) {
            if (needStop) {
                return;
            }
            Thread.sleep(period * 1000L);
            writeRecords(seq, batchSize);
            seq += batchSize;
            kv.set("sequence", String.valueOf(seq));
        }
    }

    void writeRecords(int beginSeq, int batchSize) {
        for (int i = 0; i < batchSize; i++) {
            var r = HRecord.newBuilder()
                    .put("id", beginSeq + i)
                    .put("randomInt", rnd.nextInt(1000))
                    .put("randomSmallInt", rnd.nextInt(10))
                    .put("randomDate", Instant.now().toString())
                    .build();
            ctx.send(new SourceRecord(stream, Record.newBuilder().hRecord(r).build()));
        }
    }

    @Override
    public JsonNode spec() {
        return Utils.getSpec(this, "/spec.json");
    }

    @Override
    public void stop() {
        needStop = true;
    }

    public static void main(String[] args) {
        var ctx = new SourceTaskContextImpl();
        new TaskRunner().run(args, new GeneratorSourceTask(), ctx);
    }
}
