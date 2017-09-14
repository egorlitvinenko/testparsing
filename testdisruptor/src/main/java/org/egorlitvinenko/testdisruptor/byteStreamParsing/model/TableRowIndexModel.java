package org.egorlitvinenko.testdisruptor.byteStreamParsing.model;

import org.egorlitvinenko.testdisruptor.byteStreamParsing.util.ColumnType;
import org.egorlitvinenko.testdisruptor.byteStreamParsing.util.TableRowIndexInRowCounter;

/**
 * @author Egor Litvinenko
 */
public class TableRowIndexModel {

    public final int[] indexInType;
    public final int[] int32InRow, doubleInRow, sqlDateInRow, localDateInRow, stringInRow;
    @Deprecated
    public final int[][] indexByTypeInRow;

    public TableRowIndexModel(ColumnType[] types) {
        this.indexInType = TableRowIndexInRowCounter.createIndexInType(types);
        this.indexByTypeInRow = TableRowIndexInRowCounter.createIndexByTypeInRow(types);

        int32InRow      = this.indexByTypeInRow[ColumnType.INT_32.ordinal()];
        doubleInRow     = this.indexByTypeInRow[ColumnType.DOUBLE.ordinal()];
        localDateInRow  = this.indexByTypeInRow[ColumnType.LOCAL_DATE.ordinal()];
        sqlDateInRow    = this.indexByTypeInRow[ColumnType.SQL_DATE.ordinal()];
        stringInRow     = this.indexByTypeInRow[ColumnType.STRING.ordinal()];
    }

}
