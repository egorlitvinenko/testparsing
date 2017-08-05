package org.egorlitvinenko.testdisruptor.byteStreamParsing.handler.charBufferAndcompareWithType;

import org.egorlitvinenko.testdisruptor.byteStreamParsing.model.TableRow;
import org.egorlitvinenko.testdisruptor.byteStreamParsing.parsing.api.LocalDateParserResult;
import org.egorlitvinenko.testdisruptor.byteStreamParsing.parsing.api.LocalDateParserStrategy;
import org.egorlitvinenko.testdisruptor.byteStreamParsing.util.ColumnType;

/**
 * @author Egor Litvinenko
 */
public class StringToLocalDateParseCharBufferHandler extends AbstractParseCharBufferHandler<
        LocalDateParserResult,
        LocalDateParserStrategy> {

    public StringToLocalDateParseCharBufferHandler(LocalDateParserStrategy parsingStrategy,
                                                   int[] myColumns) {
        super(parsingStrategy, myColumns, ColumnType.LOCAL_DATE);
    }

    @Override
    protected void finished(TableRow tableRow) {
        tableRow.setLocalDateIsFinished();
    }

    @Override
    protected void setValue(TableRow tableRow, int index, LocalDateParserResult result) {
        tableRow.setLocalDate(result.value, index, result.state);
    }
}
