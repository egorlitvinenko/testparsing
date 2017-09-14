package org.egorlitvinenko.testdisruptor.byteStreamParsing.event;

import org.apache.commons.lang3.NotImplementedException;
import org.egorlitvinenko.testdisruptor.byteStreamParsing.model.PrimitiveTableRow;
import org.egorlitvinenko.testdisruptor.byteStreamParsing.model.RowModel;
import org.egorlitvinenko.testdisruptor.byteStreamParsing.model.group.States;
import org.egorlitvinenko.testdisruptor.byteStreamParsing.util.ColumnType;
import org.egorlitvinenko.testdisruptor.byteStreamParsing.util.Unsafed;
import sun.misc.Unsafe;

import java.sql.Date;
import java.time.LocalDate;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Achtung, memory leaks
 *
 * @author Egor Litvinenko
 */
public class UnsafeParsePacketEvent implements PrimitiveTableRow {

    private static final Unsafe UNSAFE = Unsafed.get();

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

    private final RowModel rowModel;

    private final long int32Addresses,
            doubleAddresses,
            dateAddresses;

    private final String[] int32Strings;
    private final String[] doubleStrings;
    private final String[] localDateStrings;
    private final String[] sqlDateStrings;
    private final String[] strings;

    private AtomicBoolean doubleIsFinished;
    private AtomicBoolean int32IsFinished;
    private AtomicBoolean localDateIsFinished;
    private AtomicBoolean sqlDateIsFinished;

    private AtomicBoolean closed;

    private int id;

    public UnsafeParsePacketEvent(RowModel rowModel) {

        this.id = -1;

        this.rowModel = rowModel;

        this.dateAddresses = hasSqlDates() ? UNSAFE.allocateMemory(rowModel.sqlDateGroup.length * 3 * 4) : -1;
        this.int32Addresses = hasInt32() ? UNSAFE.allocateMemory(rowModel.int32Group.length * 4) : -1;
        this.doubleAddresses = hasDoubles() ? UNSAFE.allocateMemory(rowModel.doubleGroup.length * 8) : -1;

        this.strings = hasStrings() ? new String[rowModel.stringGroup.length] : null;

        this.localDateStrings = hasLocalDates() ? new String[rowModel.localDateGroup.length] : null;
        this.int32Strings = hasInt32() ? new String[rowModel.int32Group.length] : null;
        this.doubleStrings = hasDoubles() ? new String[rowModel.doubleGroup.length] : null;
        this.sqlDateStrings = hasSqlDates() ? new String[rowModel.sqlDateGroup.length] : null;

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
        this.strings[rowModel.indexInType[rowIndex]] = value;
    }

    public String getDoubleString(int rowIndex) {
        return doubleStrings[rowModel.indexInType[rowIndex]];
    }

    public void setDoubleString(String value, int rowIndex) {
        doubleStrings[rowModel.indexInType[rowIndex]] = value;
    }

    public void setDouble(double value, int rowIndex, byte state) {
        if (state == States.PARSED) {
            UNSAFE.putDouble(Unsafed.doubleAddress(doubleAddresses, rowModel.indexInType[rowIndex]), value);
        }
    }

    public String getInt32String(int rowIndex) {
        return int32Strings[rowModel.indexInType[rowIndex]];
    }

    public void setInt32(int value, int rowIndex, byte state) {
        if (state == States.PARSED) {
            UNSAFE.putInt(Unsafed.intAddress(int32Addresses, rowModel.indexInType[rowIndex]), value);
        }
    }

    public void setInt32String(String value, int rowIndex) {
        int32Strings[rowModel.indexInType[rowIndex]] = value;
    }

    public String getLocalDateString(int rowIndex) {
        return localDateStrings[rowModel.indexInType[rowIndex]];
    }

    public void setLocalDateString(String value, int rowIndex) {
        localDateStrings[rowModel.indexInType[rowIndex]] = value;
    }

    public void setLocalDate(LocalDate value, int rowIndex, byte state) {
        throw new NotImplementedException("1");
    }

    public String getSqlDateString(int rowIndex) {
        return sqlDateStrings[rowModel.indexInType[rowIndex]];
    }

    public void setSqlDateString(String value, int rowIndex) {
        sqlDateStrings[rowModel.indexInType[rowIndex]] = value;
    }

    @Override
    public void setSqlDate(int year, int month, int day, int rowIndex, byte state) {
        if (state == States.PARSED) {
            final int start = 3 * rowModel.indexByTypeInRow[ColumnType.SQL_DATE.ordinal()][rowIndex];
            UNSAFE.putInt(Unsafed.intAddress(this.dateAddresses, start), year);
            UNSAFE.putInt(Unsafed.intAddress(this.dateAddresses, start + 1), month);
            UNSAFE.putInt(Unsafed.intAddress(this.dateAddresses, start + 2), day);
        }
    }

    public int getIndexInRow(int type, int indexInGroup) {
        return rowModel.indexByTypeInRow[type][indexInGroup];
    }

    public boolean hasDoubles() {
        return null != rowModel.doubleGroup;
    }

    public boolean hasSqlDates() {
        return null != rowModel.sqlDateGroup;
    }

    public boolean hasInt32() {
        return null != rowModel.int32Group;
    }

    public boolean hasStrings() {
        return null != rowModel.stringGroup;
    }

    public boolean hasLocalDates() {
        return null != rowModel.localDateGroup;
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
        return hasInt32() ? this.int32Strings.length : 0;
    }

    public int doubleLength() {
        return hasDoubles() ? this.doubleStrings.length : 0;
    }

    public int localDateLength() {
        return hasLocalDates() ? this.localDateStrings.length : 0;
    }

    public int sqlDateLength() {
        return hasSqlDates() ? this.sqlDateStrings.length : 0;
    }

    public int stringLength() {
        return hasStrings() ? this.strings.length : 0;
    }

    public int getInt(int int32Index) {
        return UNSAFE.getInt(Unsafed.intAddress(int32Addresses,
                rowModel.indexByTypeInRow[ColumnType.INT_32.ordinal()][int32Index]));
    }

    public double getDouble(int doubleIndex) {
        return UNSAFE.getDouble(Unsafed.doubleAddress(doubleAddresses,
                rowModel.indexByTypeInRow[ColumnType.DOUBLE.ordinal()][doubleIndex]));
    }

    public String getString(int stringIndex) {
        return this.strings[stringIndex];
    }

    public LocalDate getLocalDate(int localDateIndex) {
        throw new NotImplementedException("1");
    }

    public Date getSqlDate(int sqlDateIndex) {
        final int start = 3 * sqlDateIndex;
        return new Date(
                UNSAFE.getInt(Unsafed.intAddress(this.dateAddresses, start)),
                UNSAFE.getInt(Unsafed.intAddress(this.dateAddresses, start + 1)),
                UNSAFE.getInt(Unsafed.intAddress(this.dateAddresses, start + 2))
        );
    }

    private int calcLength(UnsafeParsePacketEvent tableRow) {
        return this.rowModel.indexInType.length;
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
