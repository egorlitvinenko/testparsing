package org.egorlitvinenko.testdisruptor.byteStreamParsing.util;

/**
 * @author Egor Litvinenko
 */
public class TableRowIndexInRowCounter {

    public static int[] createIndexInType(ColumnType[] types) {
        int[] indexInType = new int[types.length];
        int[] countByType = new int[ColumnType.values().length];
        for (int i = 0; i < types.length; ++i) {
            indexInType[i] = countByType[types[i].ordinal()]++;
        }
        return indexInType;
    }

    public static int[][] createIndexByTypeInRow(ColumnType[] types) {
        int[][] indexInRows = new int[ColumnType.values().length][];
        int[] countByType = new int[ColumnType.values().length];
        for (int i = 0; i < types.length; ++i) {
            countByType[types[i].ordinal()]++;
        }
        for (int i = 0; i < countByType.length; ++i) {
            indexInRows[i] = new int[countByType[i]];
        }
        for (int i = 0; i < types.length; ++i) {
            countByType[types[i].ordinal()] = 0;
        }
        for (int i = 0; i < types.length; ++i) {
            indexInRows[types[i].ordinal()][countByType[types[i].ordinal()]++] = i;
        }
        return indexInRows;
    }

}
