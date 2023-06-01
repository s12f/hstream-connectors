package io.hstream.external;

import io.hstream.HArray;

public interface Source extends External {
  default String getCreateSourceConnectorConfig(String stream, String target) {
    return getCreateConnectorConfig(stream, target);
  }

  void writeDataSet(String target, HArray dataSet);
}
