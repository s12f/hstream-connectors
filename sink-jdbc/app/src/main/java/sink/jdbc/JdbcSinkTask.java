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

abstract public class JdbcSinkTask implements SinkTask {
    HRecord cfg;
    SinkTaskContext ctx;

    @Override
    public void init(HRecord config, SinkTaskContext sinkTaskContext) {
        this.cfg = config;
        this.ctx = sinkTaskContext;
    }

    @Override
    public void send(String target, List<SinkRecord> records) {
        for (var record : records) {
            var m = record.record.getDelegate().getFieldsMap();
            try {
                if (m.get("value").hasNullValue()) {
                    delete(List.of(record));
                } else {
                    upsert(List.of(record));
                }
            } catch (SQLException e) {
                throw new RuntimeException(e);
            }
        }
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
        return new ArrayList<>(structToMap(record.record.getHRecord("value").getDelegate()).keySet());
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

    abstract PreparedStatement getUpsertStmt(List<String> fields, List<String> keys);
    abstract PreparedStatement getDeleteStmt(List<String> keys);

    @Override
    public void stop() {}
}
