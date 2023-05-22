package sink.jdbc;

import io.hstream.HRecord;
import io.hstream.io.CheckResult;
import io.hstream.io.SinkRecord;
import io.hstream.io.SinkTask;
import io.hstream.io.SinkTaskContext;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.*;

import io.hstream.io.Utils;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.math3.util.Pair;

@Slf4j
abstract public class JdbcSinkTask implements SinkTask {
    HashMap<List<String>, PreparedStatement> preparedUpsertStmts = new HashMap<>();
    HashMap<List<String>, PreparedStatement> preparedDeleteStmts = new HashMap<>();
    Connection conn;

    @Override
    public void run(HRecord cfg, SinkTaskContext ctx) {
        init(cfg);
        ctx.handle((stream, records) -> handleRecordsWithException(records));
    }

    void clean() {
        preparedUpsertStmts.clear();
        preparedDeleteStmts.clear();
        conn = null;
    }

    @SneakyThrows
    void handleRecordsWithException(List<SinkRecord> records) {
        try {
            handleRecords(records);
        } catch (Throwable e) {
            clean();
            throw e;
        }
    }

    @SneakyThrows
    void handleRecords(List<SinkRecord> records) {
        var groups = splitRecords(records);
        for (var recordsPair : groups) {
            switch (recordsPair.getKey()) {
                case PLAIN_UPSERT:
                    upsertPainRecords(recordsPair.getValue());
                    return;
                case UPSERT:
                    upsert(recordsPair.getValue());
                    return;
                case DELETE:
                    delete(recordsPair.getValue());
                    return;
            }
        }
    }

    enum RecordType {
        PLAIN_UPSERT,
        UPSERT,
        DELETE
    }

    // split records for batching
    List<Pair<RecordType, List<SinkRecord>>> splitRecords(List<SinkRecord> records) {
        var result = new LinkedList<Pair<RecordType, List<SinkRecord>>>();
        var cache = new LinkedList<SinkRecord>();
        var currentType = RecordType.PLAIN_UPSERT;
        for (var record : records) {
            var m = record.record.getDelegate().getFieldsMap();
            var recordType = RecordType.PLAIN_UPSERT;
            if (isValidIORecord(record.record)) {
                recordType = m.get("value").hasNullValue() ? RecordType.DELETE : RecordType.UPSERT;
            }
            if (currentType.equals(recordType)) {
                cache.add(record);
            } else {
                if (!cache.isEmpty()) {
                    result.add(new Pair<>(currentType, cache));
                    cache.clear();
                }
                currentType = recordType;
                cache.add(record);
            }
        }
        if (!cache.isEmpty()) {
            result.add(new Pair<>(currentType, cache));
        }
        return result;
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
        var keys = getHRecordKeys(records.get(0).record.getHRecord("key"));
        PreparedStatement stmt = getUpsertStmt(fields, keys);
        for(var record : records) {
            var m = record.record.getHRecord("value").getDelegate().getFieldsMap();
            for(int i = 0; i < fields.size(); i++) {
                stmt.setObject(i + 1, Utils.pbValueToObject(m.get(fields.get(i))));
            }
            stmt.addBatch();
        }
        stmt.executeBatch();
    }

    void upsertPainRecords(List<SinkRecord> records) throws SQLException {
        var fields = getHRecordKeys(records.get(0).record);
        var keys = getPrimaryKeys();
        PreparedStatement stmt = getUpsertStmt(fields, keys);
        for(var record : records) {
            var m = record.record.getDelegate().getFieldsMap();
            for(int i = 0; i < fields.size(); i++) {
                stmt.setObject(i + 1, Utils.pbValueToObject(m.get(fields.get(i))));
            }
            stmt.addBatch();
        }
        stmt.executeBatch();
    }

    void delete(List<SinkRecord> records) throws SQLException {
        var keys = getHRecordKeys(records.get(0).record);
        PreparedStatement stmt = getDeleteStmt(keys);
        for(var record : records) {
            var m = record.record.getHRecord("key").getDelegate().getFieldsMap();
            for(int i = 0; i < keys.size(); i++) {
                stmt.setObject(i + 1, Utils.pbValueToObject(m.get(keys.get(i))));
            }
            stmt.addBatch();
        }
        stmt.executeBatch();
    }

    List<String> getFields(SinkRecord record) {
        return getHRecordKeys(record.record.getHRecord("value"));
    }

    List<String> getHRecordKeys(HRecord hRecord) {
        return new ArrayList<>(Utils.hRecordToMap(hRecord).keySet());
    }

    Connection getConn() {
        if (conn == null) {
            conn = getNewConn();
        }
        return conn;
    }

    @SneakyThrows
    PreparedStatement getUpsertStmt(List<String> fields, List<String> keys) {
        if (preparedUpsertStmts.containsKey(keys)) {
            return preparedUpsertStmts.get(keys);
        }
        var stmtSql = getUpsertSql(fields, keys);
        log.info("upsert stmtSQL:{}", stmtSql);
        var stmt = getConn().prepareStatement(stmtSql);
        preparedUpsertStmts.put(keys, stmt);
        return stmt;
    }

    @SneakyThrows
    public PreparedStatement getDeleteStmt(List<String> keys) {
        if (preparedDeleteStmts.containsKey(keys)) {
            preparedDeleteStmts.get(keys);
        }
        var stmtSql = getDeleteSql(keys);
        log.info("delete stmtSql:{}", stmtSql);
        var stmt = getConn().prepareStatement(stmtSql);
        preparedDeleteStmts.put(keys, stmt);
        return stmt;
    }

    @Override
    public CheckResult check(HRecord config) {
        try {
            init(config.getHRecord("connector"));
        } catch (Throwable e) {
            e.printStackTrace();
            log.info("check failed, {}", e.getMessage());
            return CheckResult.builder()
                    .result(false)
                    .type(CheckResult.CheckResultType.CONNECTION)
                    .message("get connector failed")
                    .build();
        }
        var keys = getPrimaryKeys();
        if (keys == null || keys.isEmpty()) {
            return CheckResult.builder()
                    .result(false)
                    .type(CheckResult.CheckResultType.KEYS)
                    .message("table/primary keys not found")
                    .build();
        }
        return CheckResult.ok();
    }


    abstract void init(HRecord cfg);
    abstract String getUpsertSql(List<String> fields, List<String> keys);
    abstract String getDeleteSql(List<String> keys);
    abstract Connection getNewConn();
    abstract List<String> getPrimaryKeys();

    @Override
    public void stop() {}
}
