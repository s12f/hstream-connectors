package io.hstream;

import lombok.extern.slf4j.Slf4j;
import org.junit.jupiter.api.*;

@Slf4j
public class ErrorTest {
  HStreamHelper helper;

  @BeforeEach
  void setup(TestInfo testInfo) throws Exception {
    helper = new HStreamHelper(testInfo);
    log.info("set up environment");
  }

  @Test
  void testConfigError() {
    var cfg = Utils.mapper.createObjectNode();
    try {
      helper.client.createConnector(
          CreateConnectorRequest.newBuilder()
              .name("sink_01")
              .type(ConnectorType.SINK)
              .target("mysql")
              .config(cfg.toString())
              .build());
    } catch (HServerException e) {
      log.info("received error:{}", e.getRawErrorBody());
    }
  }

  @AfterEach
  void tearDown() throws Exception {
    helper.close();
  }
}
