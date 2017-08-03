package org.egorlitvinenko.testdisruptor.smallstream.util;

/**
 * @author Egor Litvinenko
 */
public class ArrayUtils {

    public static Object[][] allocateObject2DArray(int batchSize, int lineSize) {
        final Object[][] result = new Object[batchSize][];
        for (int i = 0; i < batchSize; ++i) {
            result[i] = new Object[lineSize];
        }
        return result;
    }

    public static String[][] copy(String[][] array, int rows) {
        String[][] copied = new String[rows][];
        for (int i = 0; i < rows; ++i) {
            copied[i] = new String[array[i].length];
            System.arraycopy(array[i], 0, copied[i], 0, copied[i].length);
        }
        return copied;
    }

}
