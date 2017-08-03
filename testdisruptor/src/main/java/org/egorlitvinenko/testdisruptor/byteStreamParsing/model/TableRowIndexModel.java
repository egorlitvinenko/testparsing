package org.egorlitvinenko.testdisruptor.byteStreamParsing.model;

import org.egorlitvinenko.testdisruptor.byteStreamParsing.util.ColumnType;
import org.egorlitvinenko.testdisruptor.byteStreamParsing.util.TableRowIndexInRowCounter;

/**
 * @author Egor Litvinenko
 */
public class TableRowIndexModel {

    public final int[] indexInType;
    public final int[][] indexByTypeInRow;

    public TableRowIndexModel(ColumnType[] types) {
        this.indexInType = TableRowIndexInRowCounter.createIndexInType(types);
        this.indexByTypeInRow = TableRowIndexInRowCounter.createIndexByTypeInRow(types);
    }

}
