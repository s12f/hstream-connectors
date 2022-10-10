package io.hstream;

import io.hstream.external.Jdbc;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.LinkedList;
import java.util.List;
import java.util.Properties;
import java.util.Random;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;
import lombok.extern.slf4j.Slf4j;
import org.junit.jupiter.api.Assertions;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.wait.strategy.Wait;

@Slf4j
public class Utils {
    static String mysqlRootPassword = "password";
    static String postgresPassword = "postgres";
    public static GenericContainer<?> makeMysql() {
        return new GenericContainer<>("mysql")
                .withEnv("MYSQL_ROOT_PASSWORD", mysqlRootPassword)
                .withExposedPorts(3306)
                .waitingFor(Wait.forListeningPort());
    }

    public static GenericContainer<?> makePostgresql() {
        return new GenericContainer<>("postgres")
                .withEnv("POSTGRES_PASSWORD", postgresPassword)
                .withExposedPorts(5432)
                .waitingFor(Wait.forListeningPort());
    }

    public static GenericContainer<?> makeMongodb() {
        return new GenericContainer<>("mongo")
                .withExposedPorts(27017)
                .waitingFor(Wait.forListeningPort());
    }

    public static Connection getMysqlConn(int port) {
        Properties connectionProps = new Properties();
        connectionProps.put("user", "root");
        connectionProps.put("password", mysqlRootPassword);

        try {
            var conn = DriverManager.getConnection(
                    "jdbc:mysql://127.0.0.1:" + port + "/",
                    connectionProps);
            System.out.println("Connected to database");
            return conn;
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

    public static Connection getPgConn(int port, String database) {
        Properties connectionProps = new Properties();
        connectionProps.put("user", "postgres");
        connectionProps.put("password", postgresPassword);

        try {
            var conn = DriverManager.getConnection(
                    "jdbc:postgresql://127.0.0.1" + port + "/" + database,
                    connectionProps);
            System.out.println("Connected to database");
            return conn;
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

    static List<HRecord> readStream(HStreamClient client, String stream, int count, int timeout) {
        var subId = UUID.randomUUID().toString();
        client.createSubscription(Subscription.newBuilder()
                .stream(stream)
                .subscription(subId)
                .offset(Subscription.SubscriptionOffset.EARLIEST)
                .build());
        var res = new LinkedList<HRecord>();
        var latch = new CountDownLatch(count);
        var consumer = client.newConsumer().subscription(subId).hRecordReceiver((receivedHRecord, responder) -> {
            res.add(receivedHRecord.getHRecord());
            responder.ack();
            latch.countDown();
        }).build();
        consumer.startAsync().awaitRunning();
        try {
            Assertions.assertTrue(latch.await(timeout, TimeUnit.SECONDS));
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }
        consumer.stopAsync().awaitTerminated();
        return res;
    }

    static HArray randomDataSet(int num) {
        assert num > 0;
        var arrBuilder = HArray.newBuilder();
        var rand = new Random();
        for (int i = 0; i < num; i++) {
            arrBuilder.add(HRecord.newBuilder()
                    .put("key", HRecord.newBuilder().put("k1", i).build())
                    .put("value", HRecord.newBuilder()
                            .put("k1", i)
                            .put("v1", rand.nextInt(100))
                            .put("v2", UUID.randomUUID().toString())
                            .build())
                    .build()
            );
        }
        return arrBuilder.build();
    }

    static HArray randomDataSetWithoutKey(int num) {
        assert num > 0;
        var arrBuilder = HArray.newBuilder();
        var rand = new Random();
        for (int i = 0; i < num; i++) {
            arrBuilder.add(HRecord.newBuilder()
                    .put("k1", i)
                    .put("v1", rand.nextInt(100))
                    .put("v2", UUID.randomUUID().toString())
                    .build()
            );
        }
        return arrBuilder.build();
    }

    static void createTableForRandomDataSet(Jdbc jdbc, String tableName) {
        jdbc.execute(String.format("create table %s (k1 int primary key, v1 int, v2 varchar(255))", tableName));
    }

    public static String getHostname() {
        try {
            return InetAddress.getLocalHost().getHostName();
        } catch (UnknownHostException e) {
            throw new RuntimeException(e);
        }
    }

    public static void runUntil(int maxCount, int delay, Supplier<Boolean> runner) {
        assert maxCount > 0;
        int count = 0;
        while (count < maxCount) {
            try {
                Thread.sleep(delay * 1000L);
                if (runner.get()) {
                    return;
                }
                count++;
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            }
        }
        throw new RuntimeException("runUntil timeout, retried:" + count);
    }
}