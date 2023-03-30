package io.hstream.external;

public interface External {
  String getCreateConnectorConfig(String stream, String target);
  String getName();
  void close();
}
