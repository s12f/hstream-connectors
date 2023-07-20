package io.hstream.io;

public interface SinkSkipStrategy {
    boolean trySkip(SinkRecord record, String reason);
    boolean trySkipBatch(SinkRecordBatch batch, String reason);
}
