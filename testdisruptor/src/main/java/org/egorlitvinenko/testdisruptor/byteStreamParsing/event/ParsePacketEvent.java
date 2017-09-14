package org.egorlitvinenko.testdisruptor.byteStreamParsing.event;

import org.apache.commons.lang3.NotImplementedException;
import org.egorlitvinenko.testdisruptor.byteStreamParsing.adapter.RowValuesAdapter;
import org.egorlitvinenko.testdisruptor.byteStreamParsing.model.TableRow;
import org.egorlitvinenko.testdisruptor.byteStreamParsing.model.TableRowIndexModel;
import org.egorlitvinenko.testdisruptor.byteStreamParsing.model.TableRowTypeModel;
import org.egorlitvinenko.testdisruptor.byteStreamParsing.model.group.States;
import org.egorlitvinenko.testdisruptor.byteStreamParsing.util.ColumnType;

import java.sql.Date;
import java.time.LocalDate;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * @author Egor Litvinenko
 */
public class ParsePacketEvent implements TableRow {

    //region End flag
    private boolean end = false;

    public boolean isEnd() {
        return end;
    }

    public void setEnd(boolean end) {
        this.end = end;
    }
    //endregion

    public volatile int length;

    private final TableRowTypeModel typeModel;
    private final TableRowIndexModel indexModel;

    private final int[] int32s;
    private final String[] int32Strings;

    private final double[] doubles;
    private final String[] doubleStrings;

    private final LocalDate[] localDates;
    private final String[] localDateStrings;

    private final Date[] sqlDates;
    private final String[] sqlDateStrings;

    private final String[] strings;

    private AtomicBoolean doubleIsFinished;
    private AtomicBoolean int32IsFinished;
    private AtomicBoolean localDateIsFinished;
    private AtomicBoolean sqlDateIsFinished;

    private AtomicBoolean closed;

    private int id;

    public ParsePacketEvent(TableRowIndexModel indexModel,
                            TableRowTypeModel typeModel) {

        this.id = -1;

        this.typeModel = typeModel;
        this.indexModel = indexModel;

        if (hasLocalDates()) {
            this.localDates = new LocalDate[typeModel.localDateGroup.length];
            this.localDateStrings = new String[typeModel.localDateGroup.length];
        } else {
            this.localDateStrings = null;
            this.localDates = null;
        }

        if (hasInt32()) {
            this.int32s = new int[typeModel.int32Group.length];
            this.int32Strings = new String[typeModel.int32Group.length];
        } else {
            this.int32s = null;
            this.int32Strings = null;
        }

        if (hasDoubles()) {
            this.doubles = new double[typeModel.doubleGroup.length];
            this.doubleStrings = new String[typeModel.doubleGroup.length];
        } else {
            this.doubles = null;
            this.doubleStrings = null;
        }

        if (hasStrings()) {
            this.strings = new String[typeModel.stringGroup.length];
        } else {
            this.strings = null;
        }

        if (hasSqlDates()) {
            this.sqlDates = new Date[typeModel.sqlDateGroup.length];
            this.sqlDateStrings = new String[typeModel.sqlDateGroup.length];
        } else {
            this.sqlDates = null;
            this.sqlDateStrings = null;
        }


        this.localDateIsFinished = new AtomicBoolean(!hasLocalDates());
        this.doubleIsFinished = new AtomicBoolean(!hasDoubles());
        this.int32IsFinished = new AtomicBoolean(!hasInt32());
        this.sqlDateIsFinished = new AtomicBoolean(!hasSqlDates());

        this.closed = new AtomicBoolean(Boolean.FALSE);

        this.length = calcLength(this);
    }

    public int getScope() {
        return id;
    }

    public int suggestScopeAndGet(int scope) {
        if (id == -1) {
            id = scope;
        }
        return id;
    }

    public int length() {
        return this.length;
    }

    public void setString(String value, int rowIndex) {
        this.strings[indexModel.indexInType[rowIndex]] = value;
    }

    public String getDoubleString(int rowIndex) {
        return doubleStrings[indexModel.indexInType[rowIndex]];
    }

    public void setDoubleString(String value, int rowIndex) {
        doubleStrings[indexModel.indexInType[rowIndex]] = value;
    }

    public void setDouble(double value, int rowIndex, byte state) {
        if (state == States.PARSED) {
            this.doubles[indexModel.indexInType[rowIndex]] = value;
        }
    }

    public String getInt32String(int rowIndex) {
        return int32Strings[indexModel.indexInType[rowIndex]];
    }

    public void setInt32(int value, int rowIndex, byte state) {
        if (state == States.PARSED) {
            this.int32s[indexModel.indexInType[rowIndex]] = value;
        }
    }

    public void setInt32String(String value, int rowIndex) {
        int32Strings[indexModel.indexInType[rowIndex]] = value;
    }

    public String getLocalDateString(int rowIndex) {
        return localDateStrings[indexModel.indexInType[rowIndex]];
    }

    public void setLocalDateString(String value, int rowIndex) {
        localDateStrings[indexModel.indexInType[rowIndex]] = value;
    }

    public void setLocalDate(LocalDate value, int rowIndex, byte state) {
        if (state == States.PARSED) {
            this.localDates[indexModel.indexInType[rowIndex]] = value;
        }
    }

    public String getSqlDateString(int rowIndex) {
        return sqlDateStrings[indexModel.indexInType[rowIndex]];
    }

    public void setSqlDateString(String value, int rowIndex) {
        sqlDateStrings[indexModel.indexInType[rowIndex]] = value;
    }

    public void setSqlDate(Date value, int rowIndex, byte state) {
        if (state == States.PARSED) {
            this.sqlDates[indexModel.indexInType[rowIndex]] = value;
        }
    }

    public int getIndexInRow(int type, int indexInGroup) {
        return indexModel.indexByTypeInRow[type][indexInGroup];
    }

    public int getIndexInRow(ColumnType type, int indexInGroup) {
        switch (type) {
            case INT_32:
                return indexModel.int32InRow[indexInGroup];
            case DOUBLE:
                return indexModel.doubleInRow[indexInGroup];
            case LOCAL_DATE:
                return indexModel.localDateInRow[indexInGroup];
            case STRING:
                return indexModel.stringInRow[indexInGroup];
            case SQL_DATE:
                return indexModel.sqlDateInRow[indexInGroup];
            default:
                return -1;
        }
    }

    @Override
    public void visit(RowValuesAdapter rowValuesAdapter) {
        for (int i = 0; i < typeModel.types.length; ++i) {
            switch (typeModel.types[i]) {
                case INT_32:
                    rowValuesAdapter.setInt32(int32s[indexModel.indexInType[i]], i);
                    break;
                case DOUBLE:
                    rowValuesAdapter.setDouble(doubles[indexModel.indexInType[i]], i);
                    break;
                case LOCAL_DATE:
                    rowValuesAdapter.setLocalDate(localDates[indexModel.indexInType[i]], i);
                    break;
                case STRING:
                    rowValuesAdapter.setString(strings[indexModel.indexInType[i]], i);
                    break;
                case SQL_DATE:
                    rowValuesAdapter.setSqlDate(sqlDates[indexModel.indexInType[i]], i);
                    break;
            }
        }
    }

    public boolean hasDoubles() {
        return null != typeModel.doubleGroup;
    }

    public boolean hasSqlDates() {
        return null != typeModel.sqlDateGroup;
    }

    public boolean hasInt32() {
        return null != typeModel.int32Group;
    }

    public boolean hasStrings() {
        return null != typeModel.stringGroup;
    }

    public boolean hasLocalDates() {
        return null != typeModel.localDateGroup;
    }

    public void incrementProcessedElements() {
        throw new NotImplementedException("");
    }

    public void setDoublesIsFinished() {
        doubleIsFinished.set(Boolean.TRUE);
    }

    public void setInt32IsFinished() {
        int32IsFinished.set(Boolean.TRUE);
    }

    public void setLocalDateIsFinished() {
        localDateIsFinished.set(Boolean.TRUE);
    }

    public void setSqlDateIsFinished() {
        sqlDateIsFinished.set(Boolean.TRUE);
    }

    public boolean rowIsProcessed() {
        return doubleIsFinished.get()
                && int32IsFinished.get()
                && localDateIsFinished.get()
                && sqlDateIsFinished.get();
    }

    public int int32Length() {
        return this.typeModel.int32Group.length;
    }

    public int doubleLength() {
        return this.typeModel.doubleGroup.length;
    }

    public int localDateLength() {
        return null == this.typeModel.localDateGroup ? 0 : this.typeModel.localDateGroup.length;
    }

    public int sqlDateLength() {
        return this.typeModel.sqlDateGroup.length;
    }

    public int stringLength() {
        return this.typeModel.stringGroup.length;
    }

    public int getInt(int int32Index) {
        return this.int32s[int32Index];
    }

    public double getDouble(int doubleIndex) {
        return this.doubles[doubleIndex];
    }

    public String getString(int stringIndex) {
        return this.strings[stringIndex];
    }

    public LocalDate getLocalDate(int localDateIndex) {
        return this.localDates[localDateIndex];
    }

    public Date getSqlDate(int sqlDateIndex) {
        return this.sqlDates[sqlDateIndex];
    }

    private int calcLength(ParsePacketEvent tableRow) {
        return this.indexModel.indexInType.length;
    }

    public void close() {
        localDateIsFinished.set(!hasLocalDates());
        doubleIsFinished.set(!hasDoubles());
        int32IsFinished.set(!hasInt32());
        sqlDateIsFinished.set(!hasSqlDates());
        closed.set(Boolean.TRUE);
        this.id = -1;
    }

    public void reset() {
        close();
        closed.set(Boolean.FALSE);
    }

    public boolean isClosed() {
        return this.closed.get();
    }
}
