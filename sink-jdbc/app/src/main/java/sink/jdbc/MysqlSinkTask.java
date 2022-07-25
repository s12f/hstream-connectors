package sink.jdbc;

import io.hstream.HRecord;
import io.hstream.io.SinkTaskContext;
import io.hstream.io.TaskRunner;
import io.hstream.io.impl.SinkTaskContextImpl;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.List;
import java.util.Properties;

public class MysqlSinkTask extends JdbcSinkTask {
    String user;
    String password;
    int port;
    String host;
    String database;
    String table;
    Connection conn;

    @Override
    public void init(HRecord config, SinkTaskContext sinkTaskContext) {
        super.init(config, sinkTaskContext);
        this.user = cfg.getString("user");
        this.password = cfg.getString("password");
        this.host = cfg.getString("host");
        this.port = cfg.getInt("port");
        this.database = cfg.getString("database");
        this.table = cfg.getString("table");
        this.conn = getConn();
    }

    public Connection getConn() {
        Properties connectionProps = new Properties();
        connectionProps.put("user", user);
        connectionProps.put("password", password);

        try {
            var conn = DriverManager.getConnection(
                    "jdbc:mysql://" + host + ":" + port + "/" + database,
                    connectionProps);
            System.out.println("Connected to database");
            return conn;
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public PreparedStatement getUpsertStmt(List<String> fields, List<String> keys) {
        var stmt = String.format(
                "INSERT INTO %s (%s) VALUES (%s) ON DUPLICATE KEY UPDATE %s",
                table,
                Utils.makeFields(fields),
                Utils.makeValues(fields.size()),
                Utils.makeUpsertUpdate(fields));
        System.out.println("upsert statement:" + stmt);
        try {
            return conn.prepareStatement(stmt);
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public PreparedStatement getDeleteStmt(List<String> keys) {
        var stmt = String.format("delete from %s where %s", table, Utils.makeWhere(keys));
        System.out.println("delete stmt:" + stmt);
        try {
            return conn.prepareStatement(stmt);
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public void stop() {
        try {
            conn.close();
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public String spec() {
        return io.hstream.io.Utils.getSpec(this, "/sink_mysql_spec.json");
    }

    public static void main(String[] args) {
        new TaskRunner().run(args, new MysqlSinkTask(), new SinkTaskContextImpl());
    }
}
