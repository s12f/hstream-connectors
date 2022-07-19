package io.hstream.io;

import io.hstream.HRecord;

public class SinkRecord {
    public HRecord record;

    public SinkRecord(HRecord record) {
        this.record = record;
    }
}
