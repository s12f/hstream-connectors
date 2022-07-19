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

public class JdbcSinkTask implements SinkTask {
    HRecord cfg;
    SinkTaskContext ctx;
    String sinkSystem;
    final Map<String, DB> dbs = new HashMap<>();

    @Override
    public void init(HRecord config, SinkTaskContext sinkTaskContext) {
        this.cfg = config;
        this.ctx = sinkTaskContext;
        this.sinkSystem = config.getString("sink");
        initDbs(config.getString("streams"));
    }

    void initDbs(String streams) {
        var ss = io.hstream.io.impl.Utils.parseStreams(streams);
        for (var target : ss.values()) {
            DB db;
            switch (sinkSystem) {
                case "mysql":
                    db = new Mysql();
                    break;
                case "postgresql":
                    db = new Postgresql();
                    break;
                default:
                    throw new RuntimeException("unimplemented in sink-jdbc:" + sinkSystem);
            }
            db.init(Utils.parseTarget(target), cfg);
            dbs.put(target, db);
        }
    }

    DB getDB(String target) {
        return dbs.get(target);
    }

    @Override
    public void send(String target, List<SinkRecord> records) {
        for (var record : records) {
            var m = record.record.getDelegate().getFieldsMap();
            try {
                if (m.get("value").hasNullValue()) {
                delete(target, List.of(record));
                } else {
                        upsert(target, records);
                }
            } catch (SQLException e) {
                throw new RuntimeException(e);
            }
        }
    }

    void upsert(String target, List<SinkRecord> records) throws SQLException {
        var fields = getFields(records.get(0));
        var keys = getKeys(records.get(0));
        PreparedStatement stmt = getDB(target).getUpsertStmt(fields, keys);
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

    void delete(String target, List<SinkRecord> records) throws SQLException {
        var keys = getKeys(records.get(0));
        PreparedStatement stmt = getDB(target).getDeleteStmt(keys);
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

    @Override
    public void stop() {
        dbs.values().forEach(DB::close);
    }
}
