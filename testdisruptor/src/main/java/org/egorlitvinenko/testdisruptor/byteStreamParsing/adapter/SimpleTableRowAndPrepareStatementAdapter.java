package org.egorlitvinenko.testdisruptor.byteStreamParsing.adapter;

import org.egorlitvinenko.testdisruptor.byteStreamParsing.model.TableRow;
import org.egorlitvinenko.testdisruptor.byteStreamParsing.util.ColumnType;

import java.sql.Date;
import java.sql.PreparedStatement;

/**
 * @author Egor Litvinenko
 */
public class SimpleTableRowAndPrepareStatementAdapter implements TableRowAndPrepareStatementAdapter {

    // TODO check state
    @Override
    public void adopt(TableRow tableRow, PreparedStatement preparedStatement) throws Exception {
        if (tableRow.hasLocalDates()) {
            for (int i = 0, psIndex; i < tableRow.localDateLength(); ++i) {
                psIndex = tableRow.getIndexInRow(ColumnType.LOCAL_DATE, i) + 1;
                preparedStatement.setDate(psIndex, Date.valueOf(tableRow.getLocalDate(i)));
            }
        }
        if (tableRow.hasSqlDates()) {
            for (int i = 0, psIndex; i < tableRow.sqlDateLength(); ++i) {
                psIndex = tableRow.getIndexInRow(ColumnType.SQL_DATE, i) + 1;
                preparedStatement.setDate(psIndex, tableRow.getSqlDate(i));
            }
        }
        if (tableRow.hasDoubles()) {
            for (int i = 0, psIndex; i < tableRow.doubleLength(); ++i) {
                psIndex = tableRow.getIndexInRow(ColumnType.DOUBLE, i) + 1;
                preparedStatement.setDouble(psIndex, tableRow.getDouble(i));
            }
        }
        if (tableRow.hasInt32()) {
            for (int i = 0, psIndex; i < tableRow.int32Length(); ++i) {
                psIndex = tableRow.getIndexInRow(ColumnType.INT_32, i) + 1;
                preparedStatement.setInt(psIndex, tableRow.getInt(i));
            }
        }
        if (tableRow.hasStrings()) {
            for (int i = 0, psIndex; i < tableRow.stringLength(); ++i) {
                psIndex = tableRow.getIndexInRow(ColumnType.STRING, i) + 1;
                preparedStatement.setString(psIndex, tableRow.getString(i));
            }
        }
    }

}
