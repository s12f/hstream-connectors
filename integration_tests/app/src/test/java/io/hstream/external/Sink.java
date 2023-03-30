package io.hstream.external;

import io.hstream.HArray;

public interface Sink extends External {
  default String getCreateSinkConnectorConfig(String stream, String target) {
    return getCreateConnectorConfig(stream, target);
  }
  HArray readDataSet(String target);
}
