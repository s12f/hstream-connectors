package sink.elasticsearch;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.hstream.HRecord;
import io.hstream.io.SinkRecord;
import io.hstream.io.test.FakeKvStore;
import io.hstream.io.test.FakeSinkTaskContext;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.utility.DockerImageName;

import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.UUID;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

@Slf4j
public class BasicTest {
    FakeSinkTaskContext ctx;
    GenericContainer<?> es;
    EsClient esClient;
    HRecord cfg;
    String index = "index01";
    static ObjectMapper mapper = new ObjectMapper();

    @BeforeEach
    void setup() {
        es = new GenericContainer<>(DockerImageName.parse("docker.elastic.co/elasticsearch/elasticsearch:7.17.11"))
                .withEnv("discovery.type", "single-node")
                .withExposedPorts(9200)
                .withLogConsumer(log -> System.out.print(log.getUtf8String()));
        es.start();
        // cfg
        var url = "http://localhost:" + es.getFirstMappedPort();
        cfg = HRecord.newBuilder()
                .put("stream", "stream01")
                .put("url", url)
                .put("index", index)
                .build();
        // ctx
        ctx = new FakeSinkTaskContext();
        ctx.init(cfg, new FakeKvStore());

        // client
        esClient = new EsClient(url);
    }

    @SneakyThrows
    @Test
    void testFullSync() {
        ctx.appendRecords(randSinkRecords(10));
        var task = new ElasticsearchSinkTask();
        new Thread(() -> task.run(cfg, ctx)).start();
        Thread.sleep(5000);
        var rs = esClient.readRecords(index);
        log.info("rs:{}", rs);
        Assertions.assertEquals(10, rs.size());
    }

    List<SinkRecord> randSinkRecords(int count) {
        assert count > 0;
        return IntStream.range(0, count)
                .mapToObj(i -> randSinkRecord())
                .collect(Collectors.toList());

    }

    SinkRecord randSinkRecord() {
        var recordStr = mapper.createObjectNode().put("k1", UUID.randomUUID().toString()).toString();
        return SinkRecord.builder()
                .recordId(UUID.randomUUID().toString())
                .record(recordStr.getBytes(StandardCharsets.UTF_8))
                .build();
    }
}
