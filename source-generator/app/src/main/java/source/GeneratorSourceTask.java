package source;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.hstream.HRecord;
import io.hstream.Record;
import io.hstream.io.*;
import io.hstream.io.impl.SourceTaskContextImpl;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import net.jimblackler.jsongenerator.DefaultConfig;
import net.jimblackler.jsongenerator.Generator;
import net.jimblackler.jsongenerator.JsonGeneratorException;
import net.jimblackler.jsonschemafriend.SchemaStore;

import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.util.Random;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Supplier;

@Slf4j
public class GeneratorSourceTask implements SourceTask {
    static ObjectMapper mapper = new ObjectMapper();
    volatile Boolean needStop = false;
    Random rnd = new Random();
    SourceTaskContext ctx;
    KvStore kv;
    String stream;
    Supplier<Record> generator;

    @SneakyThrows
    @Override
    public void run(HRecord cfg, SourceTaskContext ctx) {
        this.ctx = ctx;
        this.kv = ctx.getKvStore();
        this.stream = cfg.getString("stream");
        var batchSize = cfg.getInt("batchSize");
        var period = cfg.getInt("period");
        var schema = "";
        if (cfg.contains("schema")) {
            schema = cfg.getString("schema");
        }
        assert batchSize > 0;
        assert period > 0;
        var seq = 0;
        var seqStr = kv.get("sequence").get();
        if (seqStr != null && !seqStr.isEmpty()) {
            seq = Integer.parseInt(seqStr);
        }
        switch (DataType.valueOf(cfg.getString("type").toUpperCase())) {
            case SEQUENCE:
                generator = getSequenceGenerator(seq);
            case JSON:
                generator = getJsonGeneratorFromSchema(schema);
        }
        while (true) {
            if (needStop) {
                return;
            }
            Thread.sleep(period * 1000L);
            writeRecords(batchSize);
            seq += batchSize;
            kv.set("sequence", String.valueOf(seq));
        }
    }

    void writeRecords(int batchSize) {
        for (int i = 0; i < batchSize; i++) {
            ctx.send(new SourceRecord(stream, generator.get()));
        }
    }

    @Override
    public JsonNode spec() {
        return Utils.getSpec(this, "/spec.json");
    }

    @SneakyThrows
    Supplier<Record> getJsonGeneratorFromSchema(String schemaStr) {
        var config = DefaultConfig.build()
                .setGenerateMinimal(false)
                .setNonRequiredPropertyChance(0.5f)
                .get();
        SchemaStore schemaStore = new SchemaStore(true);
        var schema = schemaStore.loadSchemaJson(schemaStr);
        var jsonGenerator = new Generator(config, schemaStore, new Random());
        return () -> {
            try {
                var data = jsonGenerator.generate(schema, 10);
                log.info("data:{}", data);
                return Record.newBuilder()
                        .rawRecord(mapper.valueToTree(data).toString().getBytes(StandardCharsets.UTF_8))
                        .build();
            } catch (JsonGeneratorException e) {
                throw new RuntimeException(e);
            }
        };
    }

    Supplier<Record> getSequenceGenerator(int start) {
        AtomicInteger id = new AtomicInteger(start);
        return () -> {
            var jsonData = mapper.createObjectNode()
                    .put("id", id.getAndIncrement())
                    .put("randomInt", rnd.nextInt(1000))
                    .put("randomSmallInt", rnd.nextInt(10))
                    .put("randomDate", Instant.now().toString())
                    .toString();
            return Record.newBuilder().rawRecord(jsonData.getBytes(StandardCharsets.UTF_8)).build();
        };
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
