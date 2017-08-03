package org.egorlitvinenko.testdisruptor.byteStreamParsing.util;

/**
 * @author Egor Litvinenko
 */
public class ByteSizeUtil {

    public static int ints(int n) {
        return 4 * n;
    }

    public static int longs(int n) {
        return 8 * n;
    }

    public static int doubles(int n) {
        return 8 * n;
    }

    public static int localDates(int n) {
        return (3 * 4) * n;
    }

}
