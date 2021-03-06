package org.egorlitvinenko.testdisruptor.byteStreamParsing.handler.batchAndCompareWithType;

import org.egorlitvinenko.testdisruptor.byteStreamParsing.model.TableRow;
import org.egorlitvinenko.testdisruptor.byteStreamParsing.parsing.api.SqlDateParserResult;
import org.egorlitvinenko.testdisruptor.byteStreamParsing.parsing.api.SqlDateParserStrategy;
import org.egorlitvinenko.testdisruptor.byteStreamParsing.util.ColumnType;

/**
 * @author Egor Litvinenko
 */
public class StringToSqlDateParseBatchHandler extends AbstractParseBatchHandler<
        SqlDateParserResult,
        SqlDateParserStrategy> {

    public StringToSqlDateParseBatchHandler(SqlDateParserStrategy parsingStrategy,
                                            int[] myColumns) {
        super(parsingStrategy, myColumns, ColumnType.SQL_DATE);
    }

    @Override
    protected void finished(TableRow tableRow) {
        tableRow.setSqlDateIsFinished();
    }

    @Override
    protected String getStringValue(TableRow tableRow, int index) {
        return tableRow.getSqlDateString(index);
    }

    @Override
    protected void setValue(TableRow tableRow, int index, SqlDateParserResult result) {
        tableRow.setSqlDate(result.value, index, result.state);
    }
}
