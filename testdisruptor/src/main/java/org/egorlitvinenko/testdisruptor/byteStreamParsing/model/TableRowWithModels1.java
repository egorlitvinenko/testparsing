package org.egorlitvinenko.testdisruptor.byteStreamParsing.model;

import org.egorlitvinenko.testdisruptor.byteStreamParsing.model.group.States;
import org.egorlitvinenko.testdisruptor.byteStreamParsing.model.group.StringGroup;

import java.time.LocalDate;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * @author Egor Litvinenko
 */
public class TableRowWithModels1 implements TableRow {

    public volatile int length, expectedParsedValueCount;

    protected volatile AtomicInteger settedValueCount;

    protected volatile TableRowTypeModel typeModel;
    protected volatile TableRowIndexModel indexModel;

    public TableRowWithModels1(TableRowIndexModel indexModel,
                               TableRowTypeModel typeModel) {

        this.settedValueCount = new AtomicInteger(0);

        this.typeModel = typeModel;
        this.indexModel = indexModel;

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
        settedValueCount.incrementAndGet();
        typeModel.stringGroup.values[indexModel.indexInType[rowIndex]] = value;
    }

    public String getDoubleString(int rowIndex) {
        return typeModel.doubleGroup.values[indexModel.indexInType[rowIndex]];
    }

    public void setDoubleString(String value, int rowIndex) {
        typeModel.doubleGroup.values[indexModel.indexInType[rowIndex]] = value;
    }

    public void setDouble(double value, int rowIndex, byte state) {
        if (state == States.PARSED) {
            typeModel.doubleGroup.doubles[indexModel.indexInType[rowIndex]] = value;
        }
        settedValueCount.incrementAndGet();
    }

    public String getInt32String(int rowIndex) {
        return typeModel.int32Group.values[indexModel.indexInType[rowIndex]];
    }

    public void setInt32(int value, int rowIndex, byte state) {
        if (state == States.PARSED) {
            typeModel.int32Group.int32s[indexModel.indexInType[rowIndex]] = value;
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
            typeModel.localDateGroup.dates[indexModel.indexInType[rowIndex]] = value;
        }
        if (value == null) {
            System.out.println("WTF?");
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

    public int[] getInt32s() {
        return typeModel.int32Group.int32s;
    }

    public boolean hasStrings() {
        return null != typeModel.stringGroup;
    }

    public String[] getStrings() {
        return typeModel.stringGroup.values;
    }

    public boolean hasLocalDates() {
        return null != typeModel.localDateGroup;
    }

    public LocalDate[] getLocalDates() {
        return typeModel.localDateGroup.dates;
    }

    public boolean rowIsProcessed() {
        return settedValueCount.get() == length();
    }

    @Override
    public int stringLength() {
        return typeModel.stringGroup.length;
    }

    @Override
    public int doubleLength() {
        return typeModel.doubleGroup.length;
    }

    @Override
    public int localDateLength() {
        return typeModel.localDateGroup.length;
    }

    @Override
    public int int32Length() {
        return typeModel.int32Group.length;
    }

    @Override
    public int getInt(int int32Index) {
        return typeModel.int32Group.int32s[int32Index];
    }

    @Override
    public double getDouble(int doubleIndex) {
        return typeModel.doubleGroup.doubles[doubleIndex];
    }

    @Override
    public LocalDate getLocalDate(int localDateIndex) {
        return typeModel.localDateGroup.dates[localDateIndex];
    }

    @Override
    public String getString(int stringIndex) {
        return typeModel.stringGroup.values[stringIndex];
    }

    private static int calcLength(TableRowWithModels1 tableRow) {
        return calcLength(tableRow.typeModel.stringGroup)
                + calcLength(tableRow.typeModel.doubleGroup)
                + calcLength(tableRow.typeModel.int32Group)
                + calcLength(tableRow.typeModel.localDateGroup);
    }

    private static int calcLength(StringGroup abstractGroup) {
        return Optional.ofNullable(abstractGroup).map(g -> g.length).orElse(0);
    }

}
