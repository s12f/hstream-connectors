package sink.jdbc;

import lombok.Builder;
import lombok.Data;

import java.util.List;
import java.util.Map;

@Data
@Builder
public class JdbcRecord {
    Map<String, Object> row;
    Map<String, Object> keys;
    RecordType recordType;

    public enum RecordType {
        PLAIN_UPSERT,
        UPSERT,
        DELETE
    }

}
