package io.hstream;

import io.hstream.external.Mysql;
import lombok.extern.slf4j.Slf4j;
import org.junit.jupiter.api.AfterEach;
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
    void testFullSync() throws Exception {
        Utils.testSinkFullSync(helper, mysql);
    }

    @AfterEach
    void tearDown() throws Exception {
        mysql.close();
        helper.close();
    }
}
