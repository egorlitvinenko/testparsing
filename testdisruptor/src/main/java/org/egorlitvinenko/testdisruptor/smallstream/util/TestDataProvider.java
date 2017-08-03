package org.egorlitvinenko.testdisruptor.smallstream.util;

import org.egorlitvinenko.testdisruptor.Clickhouse;
import org.egorlitvinenko.testdisruptor.byteStreamParsing.util.ColumnType;

/**
 * @author Egor Litvinenko
 */
public class TestDataProvider {


    public static final String ROW_1M__STRING_1__DOUBLE_4__ERROR_1 = "rows_1m__string_1__double_4__errors_1.csv";

    public static final Data ROW_1M__STRING_1__DOUBLE_4__ERROR_0 = new Data(
            new ColumnType[]{ColumnType.STRING, ColumnType.DOUBLE, ColumnType.DOUBLE, ColumnType.DOUBLE, ColumnType.DOUBLE},
            new byte[]{Types.STRING, Types.DOUBLE, Types.DOUBLE, Types.DOUBLE, Types.DOUBLE},
            "rows_1m__string_1__double_4__errors_0.csv",
            Clickhouse.INSERT_5_TYPED);

    public static final Data R_1M__S_1__D_4__I_4__E_0 = new Data(
            new ColumnType[]{ColumnType.STRING, ColumnType.INT_32, ColumnType.INT_32, ColumnType.INT_32, ColumnType.INT_32,
                    ColumnType.DOUBLE, ColumnType.DOUBLE, ColumnType.DOUBLE, ColumnType.DOUBLE},
            new byte[]{Types.STRING, Types.INT_32, Types.INT_32, Types.INT_32, Types.INT_32, Types.DOUBLE, Types.DOUBLE, Types.DOUBLE, Types.DOUBLE},
            "r1m__s1__d4__i4__errors_0.csv",
            "INSERT INTO test.TEST_DATA_1M_9C (ID, f1, f2, f3, f4, f5, f6, f7, f8) values (?, ?, ?, ?, ?, ?, ?, ?, ?)");

    public static final Data R_1M__S_1__DATE_1__I_4__DOUBLE_4__E_0 = new Data(
            new ColumnType[]{ColumnType.LOCAL_DATE, ColumnType.INT_32, ColumnType.INT_32, ColumnType.INT_32, ColumnType.INT_32,
                    ColumnType.DOUBLE, ColumnType.DOUBLE, ColumnType.DOUBLE, ColumnType.DOUBLE},
            new byte[]{Types.LOCAL_DATE, Types.INT_32, Types.INT_32, Types.INT_32, Types.INT_32, Types.DOUBLE, Types.DOUBLE, Types.DOUBLE, Types.DOUBLE},
            "r1m__s1__d4__i4__errors_0.csv",
            "INSERT INTO test.TEST_DATA_1M_9C_DATE (ID, f1, f2, f3, f4, f5, f6, f7, f8) values (?, ?, ?, ?, ?, ?, ?, ?, ?)");

    public static final Data R_1M__S_1__SQL_DATE_1__I_4__DOUBLE_4__E_0 = new Data(
            new ColumnType[]{ColumnType.SQL_DATE, ColumnType.INT_32, ColumnType.INT_32, ColumnType.INT_32, ColumnType.INT_32,
                    ColumnType.DOUBLE, ColumnType.DOUBLE, ColumnType.DOUBLE, ColumnType.DOUBLE},
            new byte[]{Types.LOCAL_DATE, Types.INT_32, Types.INT_32, Types.INT_32, Types.INT_32, Types.DOUBLE, Types.DOUBLE, Types.DOUBLE, Types.DOUBLE},
            "r1m__s1__d4__i4__errors_0.csv",
            "INSERT INTO test.TEST_DATA_1M_9C_DATE (ID, f1, f2, f3, f4, f5, f6, f7, f8) values (?, ?, ?, ?, ?, ?, ?, ?, ?)");

    public static final Data R_1K__S_1__DATE_1__I_4__DOUBLE_4__E_0 = new Data(
            new ColumnType[]{ColumnType.LOCAL_DATE, ColumnType.INT_32, ColumnType.INT_32, ColumnType.INT_32, ColumnType.INT_32,
                    ColumnType.DOUBLE, ColumnType.DOUBLE, ColumnType.DOUBLE, ColumnType.DOUBLE},
            new byte[]{Types.LOCAL_DATE, Types.INT_32, Types.INT_32, Types.INT_32, Types.INT_32, Types.DOUBLE, Types.DOUBLE, Types.DOUBLE, Types.DOUBLE},
            "r1k__s1__d4__i4__errors_0.csv",
            "INSERT INTO test.TEST_DATA_1M_9C_DATE (ID, f1, f2, f3, f4, f5, f6, f7, f8) values (?, ?, ?, ?, ?, ?, ?, ?, ?)");

    public static class Data {
        public final ColumnType[] columnTypes;
        public final byte[] types;
        public final String file;
        public final String insert;

        public Data(ColumnType[] columnTypes, byte[] types, String file, String insert) {
            this.columnTypes = columnTypes;
            this.types = types;
            this.file = file;
            this.insert = insert;
        }
    }

}
