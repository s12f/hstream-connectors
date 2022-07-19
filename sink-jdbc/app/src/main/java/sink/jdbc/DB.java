package sink.jdbc;

import io.hstream.HRecord;
import java.sql.PreparedStatement;
import java.util.List;

public interface DB {
    public class Target {
        public String database;
        public String table;
        Target(String database, String table) {
            this.database = database;
            this.table = table;
        }
    }

    void init(Target target, HRecord cfg);

    PreparedStatement getUpsertStmt(List<String> fields, List<String> keys);
    PreparedStatement getDeleteStmt(List<String> keys);
    void close();
}
