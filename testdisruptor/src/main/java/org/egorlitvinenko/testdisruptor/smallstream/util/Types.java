package org.egorlitvinenko.testdisruptor.smallstream.util;

import java.util.Arrays;

/**
 * @author Egor Litvinenko
 * @deprecated org.egorlitvinenko.testdisruptor.byteStreamParsing.util.ColumnType is better
 */
@Deprecated
public class Types {

    public static final byte STRING = 0;
    public static final byte DOUBLE = 1;
    public static final byte INT_32 = 2;
    public static final byte LOCAL_DATE = 3;

    public static int[] ALL = new int[]{STRING, DOUBLE, INT_32, LOCAL_DATE};

    public static int max() {
        return Arrays.stream(ALL).max().orElse(-1) + 1;
    }

}
