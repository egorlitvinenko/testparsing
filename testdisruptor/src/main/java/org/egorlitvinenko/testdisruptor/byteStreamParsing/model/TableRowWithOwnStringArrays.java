package org.egorlitvinenko.testdisruptor.byteStreamParsing.model;

import org.egorlitvinenko.testdisruptor.byteStreamParsing.model.group.States;
import org.apache.commons.lang3.NotImplementedException;

import java.sql.Date;
import java.time.LocalDate;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * @author Egor Litvinenko
 */
public class TableRowWithOwnStringArrays implements TableRow {

    public volatile int length;

    private final TableRowTypeModel typeModel;
    private final TableRowIndexModel indexModel;

    private final int[] int32s;
    private final double[] doubles;
    private final LocalDate[] localDates;
    private final Date[] sqlDates;
    private final String[] strings;

    private AtomicBoolean doubleIsFinished;
    private AtomicBoolean int32IsFinished;
    private AtomicBoolean localDateIsFinished;
    private AtomicBoolean sqlDateIsFinished;

    private AtomicBoolean closed;

    private String[] rowValues;

    public TableRowWithOwnStringArrays(TableRowIndexModel indexModel,
                                       TableRowTypeModel typeModel) {

        this.typeModel = typeModel;
        this.indexModel = indexModel;

        this.localDates = hasLocalDates() ? new LocalDate[typeModel.localDateGroup.length] : null;
        this.int32s = hasInt32() ? new int[typeModel.int32Group.length] : null;
        this.doubles = hasDoubles() ? new double[typeModel.doubleGroup.length] : null;
        this.strings = hasStrings() ? new String[typeModel.stringGroup.length] : null;
        this.sqlDates = hasSqlDates() ? new Date[typeModel.sqlDateGroup.length] : null;

        this.rowValues = new String[typeModel.types.length];

        this.localDateIsFinished = new AtomicBoolean(!hasLocalDates());
        this.doubleIsFinished = new AtomicBoolean(!hasDoubles());
        this.int32IsFinished = new AtomicBoolean(!hasInt32());
        this.sqlDateIsFinished = new AtomicBoolean(!hasSqlDates());

        this.closed = new AtomicBoolean(Boolean.FALSE);

        this.length = calcLength(this);
    }

    public int length() {
        return this.length;
    }

    public void setString(String value, int rowIndex) {
        rowValues[rowIndex] = value;
    }

    public String getDoubleString(int rowIndex) {
        return rowValues[rowIndex];
    }

    public void setDoubleString(String value, int rowIndex) {
        rowValues[rowIndex] = value;
    }

    public void setDouble(double value, int rowIndex, byte state) {
        if (state == States.PARSED) {
            this.doubles[indexModel.indexInType[rowIndex]] = value;
        }
    }

    public String getInt32String(int rowIndex) {
        return rowValues[rowIndex];
    }

    public void setInt32(int value, int rowIndex, byte state) {
        if (state == States.PARSED) {
            this.int32s[indexModel.indexInType[rowIndex]] = value;
        }
    }

    public void setInt32String(String value, int rowIndex) {
        rowValues[rowIndex] = value;
    }

    public String getLocalDateString(int rowIndex) {
        return rowValues[rowIndex];
    }

    public void setLocalDateString(String value, int rowIndex) {
        rowValues[rowIndex] = value;
    }

    public void setLocalDate(LocalDate value, int rowIndex, byte state) {
        if (state == States.PARSED) {
            this.localDates[indexModel.indexInType[rowIndex]] = value;
        }
    }

    public String getSqlDateString(int rowIndex) {
        return rowValues[rowIndex];
    }

    public void setSqlDateString(String value, int rowIndex) {
        rowValues[rowIndex] = value;
    }

    public void setSqlDate(java.sql.Date value, int rowIndex, byte state) {
        if (state == States.PARSED) {
            this.sqlDates[indexModel.indexInType[rowIndex]] = value;
        }
    }

    public int getIndexInRow(int type, int indexInGroup) {
        return indexModel.indexByTypeInRow[type][indexInGroup];
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

    private int calcLength(TableRowWithOwnStringArrays tableRow) {
        return this.indexModel.indexInType.length;
    }

    public void close() {
        localDateIsFinished.set(!hasLocalDates());
        doubleIsFinished.set(!hasDoubles());
        int32IsFinished.set(!hasInt32());
        sqlDateIsFinished.set(!hasSqlDates());
        closed.set(Boolean.TRUE);
    }

    public void reset() {
        close();
        closed.set(Boolean.FALSE);
    }

    @Override
    public boolean isClosed() {
        return this.closed.get();
    }
}
