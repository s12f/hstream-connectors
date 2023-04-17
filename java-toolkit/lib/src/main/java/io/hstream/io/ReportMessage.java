package io.hstream.io;

import lombok.Builder;
import lombok.Data;
import lombok.extern.jackson.Jacksonized;

@Data
@Jacksonized
@Builder
public class ReportMessage {
    int deliveredRecords;
    int deliveredBytes;
}
