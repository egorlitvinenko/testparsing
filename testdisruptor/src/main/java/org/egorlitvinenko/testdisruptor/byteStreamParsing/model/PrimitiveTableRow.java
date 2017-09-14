package org.egorlitvinenko.testdisruptor.byteStreamParsing.model;

import java.sql.Date;
import java.time.LocalDate;

/**
 * @author Egor Litvinenko
 */
public interface PrimitiveTableRow extends TableRow {

    default void setSqlDate(int year, int month, int day, int rowIndex, byte state) {
        throw new IllegalStateException();
    }

}
