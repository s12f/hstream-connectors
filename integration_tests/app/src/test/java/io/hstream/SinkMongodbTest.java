package io.hstream;

import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;
import com.mongodb.client.MongoCollection;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.LinkedList;
import java.util.List;
import java.util.Random;
import java.util.UUID;
import lombok.extern.slf4j.Slf4j;
import org.bson.Document;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.testcontainers.containers.GenericContainer;

@Slf4j
public class SinkMongodbTest {
    HStreamHelper helper;
    GenericContainer<?> mongodb;
    MongoClient mongoClient;
    MongoCollection<Document> collection;
    String dbStr = "d1";
    String collectionStr = "t1";

    @BeforeEach
    void setup() throws Exception {
        helper = new HStreamHelper();
        mongodb = Utils.makeMongodb();
        mongodb.start();
        log.info("Exposed ports:{}, mapping Port:{}", mongodb.getExposedPorts(), mongodb.getFirstMappedPort());

        var hosts = "127.0.0.1:" + mongodb.getFirstMappedPort();
        var authString = "";
        var connStr = String.format("mongodb://%s%s/", authString, hosts);
        mongoClient = MongoClients.create(connStr);
        var db = mongoClient.getDatabase(dbStr);
        collection = db.getCollection(collectionStr);
        log.info("connected:{}", collection.find().iterator().hasNext());
        log.info("set up environment");
    }

    @Test
    void testFullReplication() throws Exception {
        var ds = dataSet(10);
        var streamName = "stream01";
        helper.writeStream(streamName, ds);
        createConnector(streamName, dbStr, collectionStr);
        Thread.sleep(10000);
        var docs = readDocs();
        helper.deleteConnector("sk1");
        Assertions.assertEquals(10, docs.size());
    }

    List<HRecord> dataSet(int num) {
        assert num > 0;
        var ds = new LinkedList<HRecord>();
        var rand = new Random();
        for (int i = 0; i < num; i++) {
            ds.add(HRecord.newBuilder()
                    .put("key", HRecord.newBuilder().put("k1", i).build())
                    .put("value", HRecord.newBuilder()
                            .put("k1", i)
                            .put("v1", rand.nextInt(100))
                            .put("v2", UUID.randomUUID().toString())
                            .build())
                    .build()
            );
        }
        return ds;
    }

    List<Document> readDocs() throws InterruptedException {
        var docs = new LinkedList<Document>();
        try (var cursor = collection.find().iterator()) {
            while (cursor.hasNext()) {
                docs.add(cursor.next());
            }
            return docs;
        }
    }

    void createConnector(String stream, String db, String collection) throws UnknownHostException {
        var hostname = InetAddress.getLocalHost().getHostName();
        var options = new Options()
                .put("hosts", hostname + ":" + mongodb.getFirstMappedPort())
                .put("stream", stream)
                .put("database", db)
                .put("collection", collection);
        var sql = String.format("create sink connector sk1 to mongodb with (%s);", options);
        log.info("create sink mongodb sql:{}", sql);
        helper.createConnector("sk1", sql);
    }

    @AfterEach
    void tearDown() throws Exception {
        mongodb.stop();
        helper.close();
    }
}
