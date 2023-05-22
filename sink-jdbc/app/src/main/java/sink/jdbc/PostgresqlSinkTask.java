package sink.jdbc;

import com.fasterxml.jackson.databind.JsonNode;
import io.hstream.HRecord;
import io.hstream.io.TaskRunner;
import io.hstream.io.impl.SinkTaskContextImpl;
import lombok.SneakyThrows;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.List;
import java.util.Properties;
import java.util.stream.Collectors;

public class PostgresqlSinkTask extends JdbcSinkTask {
    String host;
    int port;
    String user;
    String password;
    String database;
    String table;
    List<String> primaryKeys;

    @SneakyThrows
    @Override
    public void init(HRecord cfg) {
        this.host = cfg.getString("host");
        this.port = cfg.getInt("port");
        this.user = cfg.getString("user");
        this.password = cfg.getString("password");
        this.database = cfg.getString("database");
        this.table = cfg.getString("table");
        try (var newConn = getNewConn()) {
            primaryKeys = Utils.getPrimaryKey(newConn, table);
        }
    }

    @Override
    public Connection getNewConn() {
        Properties connectionProps = new Properties();
        connectionProps.put("user", user);
        connectionProps.put("password", password);

        try {
            var conn = DriverManager.getConnection(
                    "jdbc:postgresql://" + host + ":" + port + "/" + database,
                    connectionProps);
            System.out.println("Connected to database");
            return conn;
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public String getUpsertSql(List<String> fields, List<String> keys) {
        return String.format(
                "INSERT INTO %s (%s) VALUES (%s) ON CONFLICT (%s) DO UPDATE SET %s",
                table,
                Utils.makeFields(fields),
                Utils.makeValues(fields.size()),
                Utils.makeFields(keys),
                makeUpsertUpdate(fields)
        );
    }

    @Override
    public String getDeleteSql(List<String> keys) {
        return String.format("delete from %s where %s", table, Utils.makeWhere(keys));
    }

    @Override
    List<String> getPrimaryKeys() {
        return primaryKeys;
    }

    String makeUpsertUpdate(List<String> fields) {
        return fields.stream().map(s -> String.format("%s=EXCLUDED.%s", s, s)).collect(Collectors.joining(", "));
    }

    @Override
    public JsonNode spec() {
        return io.hstream.io.Utils.getSpec(this, "/sink_mysql_spec.json");
    }

    public static void main(String[] args) {
        new TaskRunner().run(args, new PostgresqlSinkTask(), new SinkTaskContextImpl());
    }
}
