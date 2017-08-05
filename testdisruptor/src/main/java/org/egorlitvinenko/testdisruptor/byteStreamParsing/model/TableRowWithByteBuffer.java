package org.egorlitvinenko.testdisruptor.byteStreamParsing.model;

import org.apache.commons.lang3.NotImplementedException;
import org.egorlitvinenko.testdisruptor.byteStreamParsing.model.group.States;
import org.egorlitvinenko.testdisruptor.byteStreamParsing.model.group.StringGroup;
import org.egorlitvinenko.testdisruptor.byteStreamParsing.util.ByteBufferUtil;
import org.egorlitvinenko.testdisruptor.byteStreamParsing.util.ByteSizeUtil;

import java.nio.ByteBuffer;
import java.sql.Date;
import java.time.LocalDate;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * @author Egor Litvinenko
 */
public class TableRowWithByteBuffer implements TableRow {

    public volatile int length;

    private final TableRowTypeModel typeModel;
    private final TableRowIndexModel indexModel;

    private final ByteBuffer  int32s;
    private final ByteBuffer  doubles;
    private final ByteBuffer  localDates;
    private final ByteBuffer  sqlDates;
    private final String[]    strings;

    private AtomicBoolean doubleIsFinished;
    private AtomicBoolean int32IsFinished;
    private AtomicBoolean localDateIsFinished;
    private AtomicBoolean sqlDateIsFinished;

    private int id;

    public TableRowWithByteBuffer(TableRowIndexModel indexModel,
                                  TableRowTypeModel typeModel) {

        this.id = -1;

        this.typeModel = typeModel;
        this.indexModel = indexModel;

        this.localDates = hasLocalDates() ? ByteBuffer.allocate(ByteSizeUtil.localDates(typeModel.localDateGroup.length)) : null;
        this.sqlDates = hasSqlDates() ? ByteBuffer.allocate(ByteSizeUtil.localDates(typeModel.sqlDateGroup.length)) : null;
        this.int32s = hasInt32() ? ByteBuffer.allocate(ByteSizeUtil.ints(typeModel.int32Group.length)) : null;
        this.doubles = hasDoubles() ? ByteBuffer.allocate(ByteSizeUtil.doubles(typeModel.doubleGroup.length)) : null;
        this.strings = hasStrings() ? new String[typeModel.stringGroup.length] : null;

        this.localDateIsFinished = new AtomicBoolean(!hasLocalDates());
        this.doubleIsFinished = new AtomicBoolean(!hasDoubles());
        this.int32IsFinished = new AtomicBoolean(!hasInt32());
        this.sqlDateIsFinished = new AtomicBoolean(!hasSqlDates());

        this.length = calcLength(this);
    }

    public int length() {
        return this.length;
    }

    public void setString(String value, int rowIndex) {
        this.strings[indexModel.indexInType[rowIndex]] = value;
    }

    public String getDoubleString(int rowIndex) {
        return typeModel.doubleGroup.values[indexModel.indexInType[rowIndex]];
    }

    public void setDoubleString(String value, int rowIndex) {
        typeModel.doubleGroup.values[indexModel.indexInType[rowIndex]] = value;
    }

    public void setDouble(double value, int rowIndex, byte state) {
        if (state == States.PARSED) {
            ByteBufferUtil.setDoubleAtPositionInArray(this.doubles, indexModel.indexInType[rowIndex], value);
        }
    }

    public String getInt32String(int rowIndex) {
        return typeModel.int32Group.values[indexModel.indexInType[rowIndex]];
    }

    public void setInt32(int value, int rowIndex, byte state) {
        if (state == States.PARSED) {
            ByteBufferUtil.setIntAtPositionInArray(this.int32s, indexModel.indexInType[rowIndex], value);
        }
    }

    public int getIndexInRow(int type, int indexInGroup) {
        return indexModel.indexByTypeInRow[type][indexInGroup];
    }

    public boolean hasDoubles() {
        return null != typeModel.doubleGroup;
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

    @Override
    public boolean hasSqlDates() {
        return null != typeModel.sqlDateGroup;
    }

    @Override
    public void incrementProcessedElements() {
        throw new NotImplementedException("");
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

    public String getSqlDateString(int rowIndex) {
        return typeModel.sqlDateGroup.values[indexModel.indexInType[rowIndex]];
    }

    public void setSqlDateString(String value, int rowIndex) {
        typeModel.sqlDateGroup.values[indexModel.indexInType[rowIndex]] = value;
    }

    public void setLocalDate(LocalDate value, int rowIndex, byte state) {
        if (state == States.PARSED) {
            ByteBufferUtil.setLocalDateAtPositionInArray(this.localDates, indexModel.indexInType[rowIndex], value);
        }
    }

    public void setSqlDate(Date value, int rowIndex, byte state) {
        if (state == States.PARSED) {
            ByteBufferUtil.setSqlDateAtPositionInArray(this.sqlDates, indexModel.indexInType[rowIndex], value);
        }
    }

    public void setSqlDateIsFinished() {
        sqlDateIsFinished.set(Boolean.TRUE);
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

    @Override
    public boolean rowIsProcessed() {
        return doubleIsFinished.get()
                && int32IsFinished.get()
                && localDateIsFinished.get()
                && sqlDateIsFinished.get();
    }

    public int sqlDateLength() {
        return this.typeModel.sqlDateGroup.length;
    }

    public int int32Length() {
        return this.typeModel.int32Group.length;
    }

    public int doubleLength() {
        return this.typeModel.doubleGroup.length;
    }

    public int localDateLength() {
        return this.typeModel.localDateGroup.length;
    }

    public int stringLength() {
        return this.typeModel.stringGroup.length;
    }

    public int getInt(int int32Index) {
        return ByteBufferUtil.getIntAtPositionInArray(this.int32s, int32Index);
    }

    public double getDouble(int doubleIndex) {
        return ByteBufferUtil.getDoubleAtPositionInArray(this.doubles, doubleIndex);
    }

    public String getString(int stringIndex) {
        return this.strings[stringIndex];
    }

    public LocalDate getLocalDate(int localDateIndex) {
        return ByteBufferUtil.getLocalDateAtPositionInArray(this.localDates, localDateIndex);
    }

    public Date getSqlDate(int sqlDateIndex) {
        return ByteBufferUtil.getSqlDateAtPositionInArray(sqlDates, sqlDateIndex);
    }

    private int calcLength(TableRowWithByteBuffer tableRow) {
        return this.indexModel.indexInType.length;
//        return calcLength(tableRow.typeModel.stringGroup)
//                + calcLength(tableRow.typeModel.doubleGroup)
//                + calcLength(tableRow.typeModel.int32Group)
//                + calcLength(tableRow.typeModel.localDateGroup);
    }

    private int calcLength(StringGroup abstractGroup) {
        return Optional.ofNullable(abstractGroup).map(g -> g.length).orElse(0);
    }

    public void close() {
        this.doubles.clear();
        this.int32s.clear();
    }

}
