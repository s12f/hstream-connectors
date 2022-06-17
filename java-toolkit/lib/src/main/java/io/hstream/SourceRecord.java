package io.hstream;

import io.hstream.HRecord;
import io.hstream.Record;

public class SourceRecord {
    public String stream;
    public Record record;

    public SourceRecord(String stream, Record record) {
        this.stream = stream;
        this.record = record;
    }
}

