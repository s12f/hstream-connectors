package io.hstream.io;

import com.fasterxml.jackson.databind.JsonNode;
import lombok.Builder;
import lombok.Data;
import lombok.extern.jackson.Jacksonized;

import java.util.List;

@Data
@Jacksonized
@Builder
public class ReportMessage {
    int deliveredRecords;
    int deliveredBytes;
    List<JsonNode> offsets;
}
