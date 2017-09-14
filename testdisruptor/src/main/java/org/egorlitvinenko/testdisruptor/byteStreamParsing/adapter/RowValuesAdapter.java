package org.egorlitvinenko.testdisruptor.byteStreamParsing.adapter;

import java.sql.Date;
import java.time.LocalDate;

/**
 * @author Egor Litvinenko
 */
public interface RowValuesAdapter {

    void setDouble(double value, int rowIndex);
    void setInt32(int value, int rowIndex);
    void setSqlDate(Date value, int rowIndex);
    void setString(String value, int rowIndex);
    void setLocalDate(LocalDate value, int rowIndex);
    void setNull(int rowIndex);

}
