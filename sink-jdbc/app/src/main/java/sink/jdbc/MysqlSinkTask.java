package sink.jdbc;

import com.fasterxml.jackson.databind.JsonNode;
import io.hstream.HRecord;
import io.hstream.io.TaskRunner;
import io.hstream.io.impl.SinkTaskContextImpl;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.List;
import java.util.Properties;

@Slf4j
public class MysqlSinkTask extends JdbcSinkTask {
    String user;
    String password;
    int port;
    String host;
    String database;
    String table;
    List<String> primaryKeys;

    @SneakyThrows
    @Override
    public void init(HRecord cfg) {
        this.user = cfg.getString("user");
        this.password = cfg.getString("password");
        this.host = cfg.getString("host");
        this.port = cfg.getInt("port");
        this.database = cfg.getString("database");
        this.table = cfg.getString("table");
        try (var newConn = getNewConn()) {
            this.primaryKeys = Utils.getPrimaryKey(newConn, table);
        }
    }

    @Override
    public Connection getNewConn() {
        Properties connectionProps = new Properties();
        connectionProps.put("user", user);
        connectionProps.put("password", password);

        try {
            var conn = DriverManager.getConnection(
                    "jdbc:mysql://" + host + ":" + port + "/" + database,
                    connectionProps);
            log.info("Connected to database");
            return conn;
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public String getUpsertSql(List<String> fields, List<String> keys) {
        return String.format(
                "INSERT INTO %s (%s) VALUES (%s) ON DUPLICATE KEY UPDATE %s",
                table,
                Utils.makeFields(fields),
                Utils.makeValues(fields.size()),
                Utils.makeUpsertUpdate(fields));
    }

    @Override
    public String getDeleteSql(List<String> keys) {
        return String.format("delete from %s where %s", table, Utils.makeWhere(keys));
    }

    @Override
    List<String> getPrimaryKeys() {
        return primaryKeys;
    }

    @Override
    public JsonNode spec() {
        return io.hstream.io.Utils.getSpec(this, "/sink_mysql_spec.json");
    }

    public static void main(String[] args) {
        new TaskRunner().run(args, new MysqlSinkTask(), new SinkTaskContextImpl());
    }
}
