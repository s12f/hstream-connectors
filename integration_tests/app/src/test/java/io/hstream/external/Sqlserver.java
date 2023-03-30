package io.hstream.external;

import io.hstream.Options;
import io.hstream.Utils;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import lombok.extern.slf4j.Slf4j;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.wait.strategy.Wait;

@Slf4j
public class Sqlserver extends Jdbc {
  GenericContainer<?> service;
  Connection conn;
  String db = "db1";
  String user = "sa";
  String password = "Password!";

  public Sqlserver() {
    service =
        new GenericContainer<>("mcr.microsoft.com/mssql/server:2022-latest")
            .withEnv("ACCEPT_EULA", "Y")
            .withEnv("MSSQL_AGENT_ENABLED", "true")
            .withEnv("MSSQL_PID", "Standard")
            .withEnv("SA_PASSWORD", password)
            .withExposedPorts(1433)
            .waitingFor(Wait.forListeningPort());
    service.start();

    try {
      Utils.runUntil(
          3,
          3,
          () -> {
            try {
              createDatabase();
              return true;
            } catch (Exception e) {
              log.info("create database failed:{}", e.getMessage());
              e.printStackTrace();
              return false;
            }
          });
      var uri =
          String.format(
              "jdbc:sqlserver://127.0.0.1:%s;databaseName=%s;user=sa;password=%s;encrypt=false",
              service.getFirstMappedPort(), db, password);
      var conn = DriverManager.getConnection(uri);
      log.info("Connected to database");
      try (var stmt = conn.createStatement()) {
        stmt.execute("EXEC sys.sp_cdc_enable_db");
      }
      this.conn = conn;
    } catch (SQLException e) {
      throw new RuntimeException(e);
    }
  }

  public void enableCdcForTable(String table) {
    var sql =
        String.format(
            "EXEC sys.sp_cdc_enable_table @source_schema = 'dbo', @source_name = '%s', @role_name = NULL, @supports_net_changes = 0;",
            table);
    Utils.runUntil(3, 1, () -> {
      try {
        execute(sql);
        return true;
      } catch (Exception e) {
        log.info("enable CDC for table failed:{}", e.getMessage());
        e.printStackTrace();
        return false;
      }
  });
  ;
}

  @Override
  Connection getConn() {
    return conn;
  }

  @Override
  public void close() {
    try {
      conn.close();
    } catch (SQLException e) {
      throw new RuntimeException(e);
    }
    service.close();
  }

  void createDatabase() {
    var uri =
        String.format(
            "jdbc:sqlserver://127.0.0.1:%s;user=sa;password=%s;encrypt=false",
            service.getFirstMappedPort(), password);
    try (var stmt = DriverManager.getConnection(uri).createStatement()) {
      stmt.execute(String.format("create database %s", db));
    } catch (SQLException e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  public String getCreateConnectorConfig(String stream, String target) {
    var cfg = Utils.mapper.createObjectNode()
            .put("user", user)
            .put("password", password)
            .put("host", Utils.getHostname())
            .put("port", service.getFirstMappedPort())
            .put("stream", stream)
            .put("database", db)
            .put("table", target)
            .toString();
    log.info("create sqlserver connector config:{}", cfg);
    return cfg;
  }

  @Override
  public String getName() {
    return "sqlserver";
  }
}
