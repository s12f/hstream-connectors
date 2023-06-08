package sink;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.hstream.io.SinkRecord;
import lombok.Builder;
import lombok.Data;
import lombok.SneakyThrows;

import java.util.HashMap;
import java.util.Map;

@Data
@Builder
public class LasRecord {
    static ObjectMapper mapper = new ObjectMapper();

    Map<String, Object> record;

    @SneakyThrows
    public static LasRecord fromSinkRecord(SinkRecord sinkRecord) {
        return LasRecord.builder()
                .record(mapper.readValue(sinkRecord.record, new TypeReference<>() {}))
                .build();
    }
}
