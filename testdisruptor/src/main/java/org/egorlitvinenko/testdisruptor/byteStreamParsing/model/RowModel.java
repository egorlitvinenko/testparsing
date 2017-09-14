package org.egorlitvinenko.testdisruptor.byteStreamParsing.model;

import org.egorlitvinenko.testdisruptor.byteStreamParsing.model.group.DoubleGroup;
import org.egorlitvinenko.testdisruptor.byteStreamParsing.model.group.Int32Group;
import org.egorlitvinenko.testdisruptor.byteStreamParsing.model.group.LocalDateGroup;
import org.egorlitvinenko.testdisruptor.byteStreamParsing.model.group.StringGroup;
import org.egorlitvinenko.testdisruptor.byteStreamParsing.util.ColumnType;
import org.egorlitvinenko.testdisruptor.byteStreamParsing.util.TableRowIndexInRowCounter;

/**
 * @author Egor Litvinenko
 */
public class RowModel {

    public ColumnType[] types;

    public final String[] values;

    public final StringGroup stringGroup;

    public final DoubleGroup doubleGroup;
    public final Int32Group int32Group;
    public final LocalDateGroup localDateGroup;
    public final SqlDateGroup sqlDateGroup;

    public final int[] indexInType;
    public final int[][] indexByTypeInRow;

    public RowModel(ColumnType[] types) {
        this.types = types;
        final int[] lengths = TableRowTypeModel.getLength(types);
        this.values = new String[types.length];
        this.stringGroup = lengths[ColumnType.STRING.ordinal()] > 0 ? new StringGroup(lengths[ColumnType.STRING.ordinal()]) : null;
        this.doubleGroup = lengths[ColumnType.DOUBLE.ordinal()] > 0 ? new DoubleGroup(lengths[ColumnType.DOUBLE.ordinal()]) : null;
        this.int32Group = lengths[ColumnType.INT_32.ordinal()] > 0 ? new Int32Group(lengths[ColumnType.INT_32.ordinal()]) : null;
        this.localDateGroup = lengths[ColumnType.LOCAL_DATE.ordinal()] > 0 ? new LocalDateGroup(lengths[ColumnType.LOCAL_DATE.ordinal()]) : null;
        this.sqlDateGroup = lengths[ColumnType.SQL_DATE.ordinal()] > 0 ? new SqlDateGroup(lengths[ColumnType.SQL_DATE.ordinal()]) : null;

        this.indexInType = TableRowIndexInRowCounter.createIndexInType(types);
        this.indexByTypeInRow = TableRowIndexInRowCounter.createIndexByTypeInRow(types);
    }

}
