package io.hstream;

import java.util.List;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import org.junit.jupiter.api.TestInfo;

@Slf4j
public class HStreamHelper {
  String serverHost = "127.0.0.1";
  public HStreamClient client;
  public HStreamService service;
  public TestInfo testInfo;

  @SneakyThrows
  HStreamHelper(TestInfo testInfo) {
    this.testInfo = testInfo;
    service = new HStreamService();
    service.start();
    client = getClient();
    System.out.println("HStreamDB started");
  }

  @SneakyThrows
  HStreamClient getClient() {
    int retry = 0;
    while (retry < 3) {
      retry++;
      try {
        var url = "hstream://" + serverHost + ":" + service.getServerPort();
        return HStreamClient.builder().serviceUrl(url).build();
      } catch (Exception e) {
        log.info("get client failed:{}", e.getMessage());
        Thread.sleep(1000);
      }
    }
    throw new RuntimeException("get client timeout");
  }

  void writeStream(String stream, List<Record> records) {
    if (client.listStreams().stream().noneMatch(s -> s.getStreamName().equals(stream))) {
      client.createStream(stream);
    }
    try (var producer = client.newBufferedProducer().stream(stream).build()) {
      for (var record : records) {
        producer.write(record);
      }
    }
  }

  void close() throws Exception {
    service.writeLog(testInfo);
    client.close();
    service.stop();
  }
}
