package org.egorlitvinenko.testdisruptor.byteStreamParsing.model;

import org.egorlitvinenko.testdisruptor.byteStreamParsing.model.group.DoubleGroup;
import org.egorlitvinenko.testdisruptor.byteStreamParsing.model.group.Int32Group;
import org.egorlitvinenko.testdisruptor.byteStreamParsing.model.group.LocalDateGroup;
import org.egorlitvinenko.testdisruptor.byteStreamParsing.model.group.StringGroup;
import org.egorlitvinenko.testdisruptor.byteStreamParsing.util.ColumnType;

/**
 * @author Egor Litvinenko
 */
public class TableRowTypeModel {

    public ColumnType[] types;

    public final StringGroup stringGroup;

    public final DoubleGroup doubleGroup;
    public final Int32Group int32Group;
    public final LocalDateGroup localDateGroup;
    public final SqlDateGroup sqlDateGroup;

    public TableRowTypeModel(ColumnType[] types) {
        this.types = types;
        final int[] lengths = getLength(types);
        this.stringGroup = lengths[ColumnType.STRING.ordinal()] > 0 ? new StringGroup(lengths[ColumnType.STRING.ordinal()]) : null;
        this.doubleGroup = lengths[ColumnType.DOUBLE.ordinal()] > 0 ? new DoubleGroup(lengths[ColumnType.DOUBLE.ordinal()]) : null;
        this.int32Group = lengths[ColumnType.INT_32.ordinal()] > 0 ? new Int32Group(lengths[ColumnType.INT_32.ordinal()]) : null;
        this.localDateGroup = lengths[ColumnType.LOCAL_DATE.ordinal()] > 0 ? new LocalDateGroup(lengths[ColumnType.LOCAL_DATE.ordinal()]) : null;
        this.sqlDateGroup = lengths[ColumnType.SQL_DATE.ordinal()] > 0 ? new SqlDateGroup(lengths[ColumnType.SQL_DATE.ordinal()]) : null;
    }

    public static int[] getLength(ColumnType[] types) {
        int[] lengths = new int[ColumnType.values().length];
        for (int i = 0; i < types.length; ++i) {
            lengths[types[i].ordinal()]++;
        }
        return lengths;
    }

}
