package org.egorlitvinenko.testdisruptor.byteStreamParsing.model;

import org.egorlitvinenko.testdisruptor.byteStreamParsing.model.group.States;
import org.egorlitvinenko.testdisruptor.byteStreamParsing.model.group.StringGroup;
import com.google.common.util.concurrent.AtomicDoubleArray;

import java.time.LocalDate;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicIntegerArray;
import java.util.concurrent.atomic.AtomicReferenceArray;

/**
 * @author Egor Litvinenko
 */
public class TableRowWithModels2 implements TableRow {

    public volatile int length, expectedParsedValueCount;

    private final AtomicInteger settedValueCount;

    private final TableRowTypeModel typeModel;
    private final TableRowIndexModel indexModel;

    private final AtomicReferenceArray<LocalDate> localDates;
    private final AtomicIntegerArray int32s;
    private final AtomicDoubleArray doubles;
    private final AtomicReferenceArray<String> strings;

    public TableRowWithModels2(TableRowIndexModel indexModel,
                               TableRowTypeModel typeModel) {

        this.typeModel = typeModel;
        this.indexModel = indexModel;

        this.settedValueCount = new AtomicInteger(0);

        this.localDates = hasLocalDates() ? new AtomicReferenceArray<>(typeModel.localDateGroup.length) : null;
        this.int32s = hasInt32() ? new AtomicIntegerArray(typeModel.int32Group.length) : null;
        this.doubles = hasDoubles() ? new AtomicDoubleArray(typeModel.doubleGroup.length) : null;
        this.strings = hasStrings() ? new AtomicReferenceArray<>(typeModel.stringGroup.length) : null;

        this.length = calcLength(this);
        this.expectedParsedValueCount = this.length - calcLength(typeModel.stringGroup);
        if (this.length != this.indexModel.indexInType.length) {
            throw new RuntimeException("table.length != table.indexModel.indexInType.length");
        }
    }

    public int length() {
        return this.length;
    }

    public void setString(String value, int rowIndex) {
        this.strings.set(indexModel.indexInType[rowIndex], value);
        this.settedValueCount.incrementAndGet();
    }

    public String getDoubleString(int rowIndex) {
        return typeModel.doubleGroup.values[indexModel.indexInType[rowIndex]];
    }

    public void setDoubleString(String value, int rowIndex) {
        typeModel.doubleGroup.values[indexModel.indexInType[rowIndex]] = value;
    }

    public void setDouble(double value, int rowIndex, byte state) {
        if (state == States.PARSED) {
            this.doubles.set(indexModel.indexInType[rowIndex], value);
        }
        settedValueCount.incrementAndGet();
    }

    public String getInt32String(int rowIndex) {
        return typeModel.int32Group.values[indexModel.indexInType[rowIndex]];
    }

    public void setInt32(int value, int rowIndex, byte state) {
        if (state == States.PARSED) {
            this.int32s.set(indexModel.indexInType[rowIndex], value);
        }
        settedValueCount.incrementAndGet();
    }

    public void setInt32String(String value, int rowIndex) {
        typeModel.int32Group.values[indexModel.indexInType[rowIndex]] = value;
    }

    public String getLocalDateString(int rowIndex) {
        return typeModel.localDateGroup.values[indexModel.indexInType[rowIndex]];
    }

    public void setLocalDateString(String value, int rowIndex) {
        typeModel.localDateGroup.values[indexModel.indexInType[rowIndex]] = value;
    }

    public void setLocalDate(LocalDate value, int rowIndex, byte state) {
        if (state == States.PARSED) {
            this.localDates.set(indexModel.indexInType[rowIndex], value);
        }
        settedValueCount.incrementAndGet();
    }

    public int getIndexInRow(int type, int indexInGroup) {
        return indexModel.indexByTypeInRow[type][indexInGroup];
    }

    public boolean hasDoubles() {
        return null != typeModel.doubleGroup;
    }

    public double[] getDoubles() {
        return typeModel.doubleGroup.doubles;
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

    public boolean rowIsProcessed() {
        return this.settedValueCount.get() == length();
    }

    public int int32Length() {
        return this.int32s.length();
    }

    public int doubleLength() {
        return this.doubles.length();
    }

    public int localDateLength() {
        return this.localDates.length();
    }

    public int stringLength() {
        return this.strings.length();
    }

    public int getInt(int int32Index) {
        return this.int32s.get(int32Index);
    }

    public double getDouble(int doubleIndex) {
        return this.doubles.get(doubleIndex);
    }

    public String getString(int stringIndex) {
        return this.strings.get(stringIndex);
    }

    public LocalDate getLocalDate(int localDateIndex) {
        return this.localDates.get(localDateIndex);
    }

    private int calcLength(TableRowWithModels2 tableRow) {
        return calcLength(tableRow.typeModel.stringGroup)
                + calcLength(tableRow.typeModel.doubleGroup)
                + calcLength(tableRow.typeModel.int32Group)
                + calcLength(tableRow.typeModel.localDateGroup);
    }

    private int calcLength(StringGroup abstractGroup) {
        return Optional.ofNullable(abstractGroup).map(g -> g.length).orElse(0);
    }

}
