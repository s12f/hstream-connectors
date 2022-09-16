package io.hstream;

import com.google.protobuf.Struct;
import io.hstream.internal.CommandQuery;
import java.io.BufferedReader;
import java.io.File;
import java.io.InputStreamReader;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.LinkedList;
import java.util.List;
import java.util.Objects;
import java.util.Properties;
import java.util.UUID;
import lombok.extern.slf4j.Slf4j;
import org.testcontainers.containers.DockerComposeContainer;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.wait.strategy.Wait;

@Slf4j
public class Utils {
    static String mysqlRootPassword = "password";
    static String postgresPassword = "postgres";
    static String docker_compose_path = Objects.requireNonNull(Utils.class.getResource("/docker-compose.yaml")).getPath();

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

    static DockerComposeContainer<?> makeHStreamDB() throws Exception {
        return new DockerComposeContainer<>(new File(docker_compose_path))
                .withExposedService("hserver0", 6570)
                .withExposedService("hserver1", 6572)
                .withLogConsumer("hserver0", outputFrame -> log.info(outputFrame.getUtf8String()))
                .withLogConsumer("hserver1", outputFrame -> log.info(outputFrame.getUtf8String()))
                .waitingFor("hserver0", Wait.forListeningPort());
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

    static List<HRecord> readStream(HStreamClient client, String stream, int timeout) throws Exception {
        var subId = UUID.randomUUID().toString();
        client.createSubscription(Subscription.newBuilder()
                .stream(stream)
                .subscription(subId)
                .offset(Subscription.SubscriptionOffset.EARLIEST)
                .build());
        var res = new LinkedList<HRecord>();
        var consumer = client.newConsumer().subscription(subId).hRecordReceiver((receivedHRecord, responder) -> {
            res.add(receivedHRecord.getHRecord());
            responder.ack();
        }).build();
        consumer.startAsync().awaitRunning();
        Thread.sleep(timeout * 1000L);
        consumer.stopAsync().awaitTerminated();
        return res;
    }
}