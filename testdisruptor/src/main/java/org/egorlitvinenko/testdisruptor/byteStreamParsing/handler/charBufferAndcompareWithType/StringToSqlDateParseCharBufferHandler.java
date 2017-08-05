package org.egorlitvinenko.testdisruptor.byteStreamParsing.handler.charBufferAndcompareWithType;

import org.egorlitvinenko.testdisruptor.byteStreamParsing.model.TableRow;
import org.egorlitvinenko.testdisruptor.byteStreamParsing.parsing.api.SqlDateParserResult;
import org.egorlitvinenko.testdisruptor.byteStreamParsing.parsing.api.SqlDateParserStrategy;
import org.egorlitvinenko.testdisruptor.byteStreamParsing.util.ColumnType;

/**
 * @author Egor Litvinenko
 */
public class StringToSqlDateParseCharBufferHandler extends AbstractParseCharBufferHandler<
        SqlDateParserResult,
        SqlDateParserStrategy> {

    public StringToSqlDateParseCharBufferHandler(SqlDateParserStrategy parsingStrategy,
                                                 int[] myColumns) {
        super(parsingStrategy, myColumns, ColumnType.SQL_DATE);
    }

    @Override
    protected void finished(TableRow tableRow) {
        tableRow.setSqlDateIsFinished();
    }

    @Override
    protected void setValue(TableRow tableRow, int index, SqlDateParserResult result) {
        tableRow.setSqlDate(result.value, index, result.state);
    }
}
