package io.hstream.io;

import io.hstream.Record;

public class SourceRecord {
    public String stream;
    public Record record;

    public SourceRecord(String stream, Record record) {
        this.stream = stream;
        this.record = record;
    }
}

