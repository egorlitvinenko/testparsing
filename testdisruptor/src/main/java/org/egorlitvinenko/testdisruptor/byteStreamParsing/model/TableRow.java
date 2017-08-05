package org.egorlitvinenko.testdisruptor.byteStreamParsing.model;

import java.sql.Date;
import java.time.LocalDate;

/**
 * @author Egor Litvinenko
 */
public interface TableRow extends AutoCloseable {

    default int getScope() {
        throw new IllegalStateException();
    }

    default int suggestScopeAndGet(int scope) {
        throw new IllegalStateException();
    }

    default void setDoublesIsFinished() {
        throw new IllegalStateException();
    }

    default void setLocalDateIsFinished() {
        throw new IllegalStateException();
    }

    default void setInt32IsFinished() {
        throw new IllegalStateException();
    }

    default void setSqlDateIsFinished() {
        throw new IllegalStateException();
    }

    default void incrementProcessedElements() {
        throw new IllegalStateException();
    }

    int length();

    void setString(String value, int rowIndex);

    String getDoubleString(int rowIndex);

    void setDoubleString(String value, int rowIndex);

    void setDouble(double value, int rowIndex, byte state);

    String getInt32String(int rowIndex);

    void setInt32(int value, int rowIndex, byte state);

    void setInt32String(String value, int rowIndex);

    String getLocalDateString(int rowIndex);

    void setLocalDateString(String value, int rowIndex);

    void setLocalDate(LocalDate value, int rowIndex, byte state);

    default String getSqlDateString(int rowIndex) {
        throw new IllegalStateException();
    }

    default void setSqlDateString(String value, int rowIndex) {
        throw new IllegalStateException();
    }

    default void setSqlDate(java.sql.Date value, int rowIndex, byte state) {
        throw new IllegalStateException();
    }

    int getIndexInRow(int type, int indexInGroup);

    boolean hasDoubles();

    boolean hasInt32();

    boolean hasStrings();

    boolean hasLocalDates();

    default boolean hasSqlDates() {
        throw new IllegalStateException();
    }

    boolean rowIsProcessed();

    int stringLength();

    int doubleLength();

    int localDateLength();

    int int32Length();

    default int sqlDateLength() {
        throw new IllegalStateException();
    }

    int getInt(int int32Index);

    double getDouble(int doubleIndex);

    LocalDate getLocalDate(int localDateIndex);

    default Date getSqlDate(int sqlDateIndex) {
        throw new IllegalStateException();
    }

    String getString(int stringIndex);

    default void close() {

    }

    default void reset() {

    }

    default boolean isClosed() {
        throw new IllegalStateException();
    }

}
