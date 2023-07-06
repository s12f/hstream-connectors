package io.hstream.io;

import lombok.Builder;
import lombok.Getter;

@Getter
@Builder
public class SinkRecord {
    public byte[] record;
    String recordId;
}
