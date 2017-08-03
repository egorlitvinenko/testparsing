package org.egorlitvinenko.testflink;

import org.apache.flink.api.java.tuple.Tuple5;
import org.apache.flink.api.java.tuple.Tuple9;
import org.apache.flink.types.Either;
import org.apache.flink.types.Row;

import java.time.LocalDate;
import java.time.format.DateTimeFormatter;

/**
 * @author Egor Litvinenko
 */
public class RowParser {
    private static DateTimeFormatter DATE_FORMATTER = DateTimeFormatter.ISO_LOCAL_DATE;

    public static Result parse(Tuple5<String, String, String, String, String> tuple5) {
        boolean success = true;
        for (int i = 1; i < 5 && success; ++i) {
            success = success && test(tuple5.getField(i));
        }
        return new Result(
                Row.of(tuple5.getField(0),
                        tuple5.getField(1),
                        tuple5.getField(2),
                        tuple5.getField(3),
                        tuple5.getField(4),
                        Integer.valueOf(success ? 1 : 0)),
                success);
    }

    public static Result parse(Tuple9<String, String, String, String, String, String, String, String, String> tuple9) {
        return new Result(
                Row.of(LocalDate.from(DATE_FORMATTER.parse(tuple9.getField(0))),
                        Integer.valueOf(tuple9.getField(1)),
                        Integer.valueOf(tuple9.getField(2)),
                        Integer.valueOf(tuple9.getField(3)),
                        Integer.valueOf(tuple9.getField(4)),
                        Double.valueOf(tuple9.getField(5)),
                        Double.valueOf(tuple9.getField(6)),
                        Double.valueOf(tuple9.getField(7)),
                        Double.valueOf(tuple9.getField(8))),
                true);
    }

    public static Result2 parse2(Tuple5<String, String, String, String, String> tuple5) {
        Object[] newRow = new Object[5];
        newRow[0] = tuple5.getField(0);
        boolean success = true;
        for (int i = 1; i < 5 && success; ++i) {
            newRow[i] = test2(tuple5.getField(i));
            success = success && null != newRow[i];
            if (!success) {
                break;
            }
        }
        return new Result2(
                success
                        ? Either.Right(Row.of(newRow))
                        : Either.Left(
                        Row.of(tuple5.getField(0),
                                tuple5.getField(1),
                                tuple5.getField(2),
                                tuple5.getField(3),
                                tuple5.getField(4))),
                success);
    }

    public static Result2 parse2(Tuple9<String, String, String, String, String, String, String, String, String> tuple5) {
        final int length = 9;
        Object[] newRow = new Object[length];
        try {
            int i = 0;
            newRow[i] = LocalDate.from(DATE_FORMATTER.parse(tuple5.getField(i++)));

            newRow[i] = Integer.valueOf(tuple5.getField(i++));
            newRow[i] = Integer.valueOf(tuple5.getField(i++));
            newRow[i] = Integer.valueOf(tuple5.getField(i++));
            newRow[i] = Integer.valueOf(tuple5.getField(i++));

            newRow[i] = Double.valueOf(tuple5.getField(i++));
            newRow[i] = Double.valueOf(tuple5.getField(i++));
            newRow[i] = Double.valueOf(tuple5.getField(i++));
            newRow[i] = Double.valueOf(tuple5.getField(i));
            return new Result2(Either.Right(Row.of(newRow)), true);
        } catch (Exception e) {
            e.printStackTrace();
            int i = 0;
            return new Result2(Either.Left(
                    Row.of(tuple5.getField(i++),
                            tuple5.getField(i++),
                            tuple5.getField(i++),
                            tuple5.getField(i++),
                            tuple5.getField(i++),
                            tuple5.getField(i++),
                            tuple5.getField(i++),
                            tuple5.getField(i++),
                            tuple5.getField(i++))
            ), false);
        }
    }

    public static Result parse3(Tuple5<String, String, String, String, String> tuple5) {
        Object[] newRow = new Object[6];
        newRow[0] = tuple5.getField(0);
        boolean success = true;
        for (int i = 1; i < 5 && success; ++i) {
            newRow[i] = test2(tuple5.getField(i));
            success = success && null != newRow[i];
            if (!success) {
                break;
            }
        }
        newRow[5] = success;
        return new Result(
                success
                        ? Row.of(newRow)
                        : Row.of(tuple5.getField(0),
                        tuple5.getField(1),
                        tuple5.getField(2),
                        tuple5.getField(3),
                        tuple5.getField(4),
                        false),
                success);
    }

    private static boolean test(Object value) {
        try {
            Double.parseDouble(String.valueOf(value));
            return true;
        } catch (NumberFormatException e) {
            return false;
        }
    }

    private static Double test2(Object value) {
        try {
            return Double.valueOf(String.valueOf(value));
        } catch (NumberFormatException e) {
            return null;
        }
    }

    public static class Result {
        public final Row row;
        public final boolean success;

        public Result(Row row, boolean success) {
            this.row = row;
            this.success = success;
        }
    }


    public static class Result2 {
        public final Either<Row, Row> result;
        public final boolean success;

        public Result2(Either<Row, Row> result, boolean success) {
            this.result = result;
            this.success = success;
        }
    }

}
