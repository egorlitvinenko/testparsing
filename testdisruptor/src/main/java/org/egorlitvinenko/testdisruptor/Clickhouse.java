package org.egorlitvinenko.testdisruptor;

import ru.yandex.clickhouse.ClickHouseDataSource;

import java.sql.Connection;
import java.sql.SQLException;

/**
 * @author Egor Litvinenko
 */
public class Clickhouse {

    public static final String INSERT_5_STRINGS = "INSERT INTO test.TEST_DATA_1M_STRING (ID, f1, f2, f3, f4) values (?, ?, ?, ?, ?)";

    public static final String INSERT_5_TYPED = "INSERT INTO test.TEST_DATA_1M (ID, f1, f2, f3, f4) values (?, ?, ?, ?, ?)";

    public static final String INSERT_9_TYPED = "INSERT INTO test.TEST_DATA_1M_9C (ID, f1, f2, f3, f4, f5, f6, f7, f8) values (?, ?, ?, ?, ?, ?, ?, ?, ?)";

    private static final Clickhouse INSTANCE = new Clickhouse();

    public static Clickhouse it() {
        return INSTANCE;
    }

    private final ClickHouseDataSource clickHouseDataSource;

    private Clickhouse() {
        this.clickHouseDataSource = new
                ClickHouseDataSource("jdbc:clickhouse://localhost:9123");
    }

    public Connection getConnection() {
        try {
            return this.clickHouseDataSource.getConnection();
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

    public static void setApacheHttpClientLoggingSettings() {
        System.setProperty("org.apache.commons.logging.Log", "org.apache.commons.logging.impl.SimpleLog");
        System.setProperty("org.apache.commons.logging.simplelog.showdatetime", "true");
        System.setProperty("org.apache.commons.logging.simplelog.log.org.apache.http.impl.conn", "ERROR");
        System.setProperty("org.apache.commons.logging.simplelog.log.org.apache.http", "ERROR");
    }

}
