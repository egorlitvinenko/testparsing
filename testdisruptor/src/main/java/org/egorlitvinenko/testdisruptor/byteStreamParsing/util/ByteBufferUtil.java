package org.egorlitvinenko.testdisruptor.byteStreamParsing.util;

import java.nio.ByteBuffer;
import java.time.LocalDate;

/**
 * @author Egor Litvinenko
 */
public class ByteBufferUtil {

    public static int getIntAtPositionInArray(ByteBuffer byteBuffer, int pos) {
        return byteBuffer.getInt(ByteSizeUtil.ints(pos));
    }

    public static void setIntAtPositionInArray(ByteBuffer byteBuffer, int pos, int value) {
        byteBuffer.putInt(ByteSizeUtil.ints(pos), value);
    }

    public static double getDoubleAtPositionInArray(ByteBuffer byteBuffer, int pos) {
        return byteBuffer.getDouble(ByteSizeUtil.doubles(pos));
    }

    public static void setDoubleAtPositionInArray(ByteBuffer byteBuffer, int pos, double value) {
        byteBuffer.putDouble(ByteSizeUtil.doubles(pos), value);
    }

    public static LocalDate getLocalDateAtPositionInArray(ByteBuffer byteBuffer, int pos) {
        int start = ByteSizeUtil.localDates(pos);
        try {
            return LocalDate.of(byteBuffer.getInt(start), byteBuffer.getInt(start + 4), byteBuffer.getInt(start + 8));
        } catch (Exception e) {
            System.out.println("null");
            throw e;
        }
    }

    public static void setLocalDateAtPositionInArray(ByteBuffer byteBuffer, int pos, LocalDate value) {
        int start = ByteSizeUtil.localDates(pos);
        byteBuffer.putInt(start, value.getYear());
        byteBuffer.putInt(start + 4, value.getMonthValue());
        byteBuffer.putInt(start + 8, value.getDayOfMonth());
    }

}
