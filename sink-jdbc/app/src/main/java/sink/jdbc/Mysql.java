package sink.jdbc;

import io.hstream.HRecord;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.List;
import java.util.Properties;

public class Mysql implements DB {
    String user;
    String password;
    int port;
    String host;
    Target target;
    Connection conn;

    @Override
    public void init(Target target, HRecord cfg) {
        this.user = cfg.getString("user");
        this.password = cfg.getString("password");
        this.host = cfg.getString("host");
        this.port = cfg.getInt("port");
        this.target = target;
        this.conn = getConn();
    }

    public Connection getConn() {
        Properties connectionProps = new Properties();
        connectionProps.put("user", user);
        connectionProps.put("password", password);

        try {
            var conn = DriverManager.getConnection(
                    "jdbc:mysql://" + host + ":" + port + "/" + target.database,
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
                target.table,
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
        var stmt = String.format("delete from %s where %s", target.table, Utils.makeWhere(keys));
        System.out.println("delete stmt:" + stmt);
        try {
            return conn.prepareStatement(stmt);
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public void close() {
        try {
            conn.close();
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }
}
