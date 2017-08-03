package org.egorlitvinenko.testdisruptor.byteStreamParsing.factory;

import org.egorlitvinenko.testdisruptor.byteStreamParsing.model.TableRow;

import java.time.LocalDate;

/**
 * @author Egor Litvinenko
 */
public class ArrayPooledTableRow implements TableRow {

    private final TableRow delegate;
    private final ArrayTableRowPool myPool;
    private final int myPoolIndex;

    public ArrayPooledTableRow(ArrayTableRowPool pool, int poolIndex, TableRow delegate) {
        this.delegate = delegate;
        this.myPool = pool;
        this.myPoolIndex = poolIndex;
    }

    public int getMyPoolIndex() {
        return myPoolIndex;
    }

    @Override
    public int length() {
        return delegate.length();
    }

    @Override
    public void setString(String value, int rowIndex) {
        delegate.setString(value, rowIndex);
    }

    @Override
    public String getDoubleString(int rowIndex) {
        return delegate.getDoubleString(rowIndex);
    }

    @Override
    public void setDoubleString(String value, int rowIndex) {
        delegate.setDoubleString(value, rowIndex);
    }

    @Override
    public void setDouble(double value, int rowIndex, byte state) {
        delegate.setDouble(value, rowIndex, state);
    }

    @Override
    public String getInt32String(int rowIndex) {
        return delegate.getInt32String(rowIndex);
    }

    @Override
    public void setInt32(int value, int rowIndex, byte state) {
        delegate.setInt32(value, rowIndex, state);
    }

    @Override
    public void setInt32String(String value, int rowIndex) {
        delegate.setInt32String(value, rowIndex);
    }

    @Override
    public String getLocalDateString(int rowIndex) {
        return delegate.getLocalDateString(rowIndex);
    }

    @Override
    public void setLocalDateString(String value, int rowIndex) {
        delegate.setLocalDateString(value, rowIndex);
    }

    @Override
    public void setLocalDate(LocalDate value, int rowIndex, byte state) {
        delegate.setLocalDate(value, rowIndex, state);
    }

    @Override
    public int getIndexInRow(int type, int indexInGroup) {
        return delegate.getIndexInRow(type, indexInGroup);
    }

    @Override
    public boolean hasDoubles() {
        return delegate.hasDoubles();
    }

    @Override
    public boolean hasInt32() {
        return delegate.hasInt32();
    }

    @Override
    public boolean hasStrings() {
        return delegate.hasStrings();
    }

    @Override
    public boolean hasLocalDates() {
        return delegate.hasLocalDates();
    }

    @Override
    public void incrementProcessedElements() {
        delegate.incrementProcessedElements();
    }

    @Override
    public boolean rowIsProcessed() {
        return delegate.rowIsProcessed();
    }

    @Override
    public int stringLength() {
        return delegate.stringLength();
    }

    @Override
    public int doubleLength() {
        return delegate.doubleLength();
    }

    @Override
    public int localDateLength() {
        return delegate.localDateLength();
    }

    @Override
    public int int32Length() {
        return delegate.int32Length();
    }

    @Override
    public int getInt(int int32Index) {
        return delegate.getInt(int32Index);
    }

    @Override
    public double getDouble(int doubleIndex) {
        return delegate.getDouble(doubleIndex);
    }

    @Override
    public LocalDate getLocalDate(int localDateIndex) {
        return delegate.getLocalDate(localDateIndex);
    }

    @Override
    public String getString(int stringIndex) {
        return delegate.getString(stringIndex);
    }

    @Override
    public void close() {
        delegate.close();
        myPool.free(myPoolIndex);
    }
}
