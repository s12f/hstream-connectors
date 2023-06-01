package io.hstream;

import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import org.junit.jupiter.api.*;
import org.testcontainers.shaded.com.fasterxml.jackson.databind.ObjectMapper;

@Slf4j
public class SourceGeneratorTest {
  HStreamHelper helper;
  static ObjectMapper mapper = new ObjectMapper();

  @BeforeEach
  void setup(TestInfo testInfo) {
    helper = new HStreamHelper(testInfo);
  }

  @SneakyThrows
  @AfterEach
  void tearDown() {
    helper.close();
  }

  @SneakyThrows
  @Test
  void testFullSync() {
    var cfg =
        mapper
            .createObjectNode()
            .put("stream", "stream01")
            .put("type", "sequence")
            .put("batchSize", 1)
            .put("period", 1);
    var connector =
        helper.client.createConnector(
            CreateConnectorRequest.newBuilder()
                .name("src01")
                .type(ConnectorType.valueOf("SOURCE"))
                .target("generator")
                .config(cfg.toString())
                .build());
    log.info("created connector:{}", connector);
    Thread.sleep(5000);
    var result = Utils.readStream(helper.client, "stream01", 10, 15);
    var seq = result.size() - 1;
    Assertions.assertEquals(seq, result.get(seq).getInt("id"));
  }
}
