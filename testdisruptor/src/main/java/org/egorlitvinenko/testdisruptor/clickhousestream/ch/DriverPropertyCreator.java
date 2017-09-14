package org.egorlitvinenko.testdisruptor.clickhousestream.ch;

import java.sql.DriverPropertyInfo;
import java.util.Properties;

public interface DriverPropertyCreator {
    DriverPropertyInfo createDriverPropertyInfo(Properties properties);
}
