package org.egorlitvinenko.testdisruptor.byteStreamParsing.util;

/**
 * @author Egor Litvinenko
 */
public enum ColumnType {

    INT_32,
    DOUBLE,
    LOCAL_DATE,
    STRING,
    SQL_DATE;

    public static int count(ColumnType[] types, ColumnType type) {
        int sum = 0;
        for (int i = 0; i < types.length; i++) {
            if (types[i] == type) sum++;
        }
        return sum;
    }

}
