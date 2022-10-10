package io.hstream;

import io.hstream.external.Mysql;
import lombok.extern.slf4j.Slf4j;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInfo;

@Slf4j
public class SinkMysqlTest {
    HStreamHelper helper;
    Mysql mysql;

    @BeforeEach
    void setup(TestInfo testInfo) throws Exception {
        helper = new HStreamHelper(testInfo);
        mysql = new Mysql();
        log.info("set up environment");
    }

    @Test
    void testFullReplication() throws Exception {
        var ds = Utils.randomDataSet(10);
        var streamName = "stream01";
        helper.writeStream(streamName, ds);
        var connectorName = "sk1";
        var table = "t1";
        Utils.createTableForRandomDataSet(mysql, table);
        var sql = mysql.createSinkConnectorSql(connectorName, streamName, table);
        helper.createConnector(streamName, sql);
        Utils.runUntil(10, 3, () -> {
            var dataSet = mysql.readDataSet(table);
            return dataSet.size() >= 10;
        });
        // wait and re-check
        Thread.sleep(1000);
        var dataSet = mysql.readDataSet(table);
        Assertions.assertEquals(10, dataSet.size());
        helper.deleteConnector(connectorName);
    }

    @AfterEach
    void tearDown() throws Exception {
        mysql.close();
        helper.close();
    }
}
