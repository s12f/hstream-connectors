package io.hstream.io;

import lombok.Builder;

@Builder
public class SinkRecord {
    public byte[] record;

    public SinkRecord(byte[] record) {
        this.record = record;
    }
}
