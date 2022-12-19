package sink.jdbc;

import com.google.protobuf.Struct;
import com.google.protobuf.Value;
import io.hstream.HRecord;
import io.hstream.io.SinkRecord;
import io.hstream.io.SinkTask;
import io.hstream.io.SinkTaskContext;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import lombok.extern.slf4j.Slf4j;

@Slf4j
abstract public class JdbcSinkTask implements SinkTask {
    @Override
    public void run(HRecord cfg, SinkTaskContext ctx) {
        init(cfg);
        ctx.handle((stream, records) -> {
            for (var record : records) {
                var m = record.record.getDelegate().getFieldsMap();
                try {
                    if (!isValidIORecord(record.record)) {
                        upsertPainRecords(List.of(record));
                    } else if (m.get("value").hasNullValue()) {
                        delete(List.of(record));
                    } else {
                        upsert(List.of(record));
                    }
                } catch (SQLException e) {
                    throw new RuntimeException(e);
                }
            }
        });
    }

    boolean isValidIORecord(HRecord hRecord) {
        if (hRecord.contains("key") && hRecord.contains("value")) {
            try {
                hRecord.getHRecord("key");
                return true;
            } catch (Exception e) {
                return false;
            }
        }
        return false;
    }

    void upsert(List<SinkRecord> records) throws SQLException {
        var fields = getFields(records.get(0));
        var keys = getKeys(records.get(0));
        PreparedStatement stmt = getUpsertStmt(fields, keys);
        for(var record : records) {
            var m = record.record.getHRecord("value").getDelegate().getFieldsMap();
            for(int i = 0; i < fields.size(); i++) {
                stmt.setObject(i + 1, valueToObject(m.get(fields.get(i))));
            }
            stmt.addBatch();
        }
        stmt.executeBatch();
        stmt.close();
    }

    void upsertPainRecords(List<SinkRecord> records) throws SQLException {
        var fields = getHRecordFields(records.get(0).record);
        var keys = getPrimaryKeys();
        log.info("primary keys:{}", keys);
        if (keys.isEmpty()) {
            throw new RuntimeException("primary keys is empty");
        }
        PreparedStatement stmt = getUpsertStmt(fields, keys);
        for(var record : records) {
            var m = record.record.getDelegate().getFieldsMap();
            for(int i = 0; i < fields.size(); i++) {
                stmt.setObject(i + 1, valueToObject(m.get(fields.get(i))));
            }
            stmt.addBatch();
        }
        stmt.executeBatch();
        stmt.close();
    }

    void delete(List<SinkRecord> records) throws SQLException {
        var keys = getKeys(records.get(0));
        PreparedStatement stmt = getDeleteStmt(keys);
        for(var record : records) {
            var m = record.record.getHRecord("key").getDelegate().getFieldsMap();
            for(int i = 0; i < keys.size(); i++) {
                stmt.setObject(i + 1, valueToObject(m.get(keys.get(i))));
            }
            stmt.addBatch();
        }
        stmt.executeBatch();
        stmt.close();
    }

    List<String> getKeys(SinkRecord record) {
        return new ArrayList<>(structToMap(record.record.getHRecord("key").getDelegate()).keySet());
    }

    List<String> getFields(SinkRecord record) {
        return getHRecordFields(record.record.getHRecord("value"));
    }

    List<String> getHRecordFields(HRecord hRecord) {
        return new ArrayList<>(structToMap(hRecord.getDelegate()).keySet());
    }

    public Object valueToObject(Value value) {
        switch (value.getKindCase()) {
            case NULL_VALUE:
                return null;
            case NUMBER_VALUE:
                return value.getNumberValue();
            case STRING_VALUE:
                return value.getStringValue();
            case BOOL_VALUE:
                return value.getBoolValue();
            case LIST_VALUE:
                return value.getListValue().getValuesList().stream().map(this::valueToObject).collect(Collectors.toList());
            case STRUCT_VALUE:
                return structToMap(value.getStructValue());
            default:
                return new RuntimeException("invalid value:" + value);
        }
    }

    public Map<String, Object> structToMap(Struct struct) {
        var m = new HashMap<String, Object>();
        for (var entry : struct.getFieldsMap().entrySet()) {
            m.put(entry.getKey(), valueToObject(entry.getValue()));
        }
        return m;
    }

    abstract void init(HRecord cfg);
    abstract PreparedStatement getUpsertStmt(List<String> fields, List<String> keys);
    abstract PreparedStatement getDeleteStmt(List<String> keys);
    abstract List<String> getPrimaryKeys();

    @Override
    public void stop() {}
}
