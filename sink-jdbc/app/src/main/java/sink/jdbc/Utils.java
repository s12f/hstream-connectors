package sink.jdbc;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.ResultSet;
import java.util.*;
import java.util.stream.Collectors;

import io.hstream.io.SinkRecord;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class Utils {
    static ObjectMapper mapper = new ObjectMapper();

    static String makeFields(List<String> fields) {
        return String.join(", ", fields);
    }

    static String makeValues(int size) {
        return String.join(", ", Collections.nCopies(size, "?"));
    }

    static String makeUpsertUpdate(List<String> fields) {
        return fields.stream().map(s -> String.format("%s=VALUES(%s)", s, s)).collect(Collectors.joining(", "));
    }

    @SneakyThrows
    static List<String> getPrimaryKey(Connection conn, String table) {
        DatabaseMetaData dmd = conn.getMetaData();
        ResultSet rs = dmd.getPrimaryKeys(null, null, table);

        var keys = new LinkedList<String>();
        while(rs.next()){
            keys.add(rs.getString("COLUMN_NAME"));
        }
        return keys;
    }

    static String makeWhere(List<String> fields) {
        return fields.stream().map(s -> String.format("%s=?", s)).collect(Collectors.joining(", "));
    }

    @SneakyThrows
    static JdbcRecord jdbcRecordFromSinkRecord(SinkRecord record) {
        var mapRecord = mapper.readValue(record.record, new TypeReference<Map<String, Object>>() {});
        if (isValidIORecord(mapRecord)) {
            var recordBuilder = JdbcRecord.builder();
            recordBuilder.keys((Map<String, Object>) mapRecord.get("key"));
            if (mapRecord.get("value") == null) {
                recordBuilder.recordType(JdbcRecord.RecordType.DELETE);
                recordBuilder.row(null);
            } else {
                recordBuilder.recordType(JdbcRecord.RecordType.UPSERT);
                recordBuilder.row((Map<String, Object>) mapRecord.get("value"));
            }
            return recordBuilder.build();
        }
        return JdbcRecord.builder()
                .row(mapRecord)
                .keys(null)
                .recordType(JdbcRecord.RecordType.PLAIN_UPSERT)
                .build();
    }

    static boolean isValidIORecord(Map<String, Object> mapRecord) {
        if (mapRecord.containsKey("key") && mapRecord.containsKey("value")) {
            return mapRecord.get("key") instanceof Map;
        }
        return false;
    }
}
