package org.egorlitvinenko.testdisruptor.byteStreamParsing.handler.charBufferAndcompareWithType;

import org.egorlitvinenko.testdisruptor.byteStreamParsing.model.TableRow;
import org.egorlitvinenko.testdisruptor.byteStreamParsing.parsing.api.DoubleParserResult;
import org.egorlitvinenko.testdisruptor.byteStreamParsing.parsing.api.DoubleParsingStrategy;
import org.egorlitvinenko.testdisruptor.byteStreamParsing.util.ColumnType;

/**
 * @author Egor Litvinenko
 */
public class StringToDoubleParseCharBufferHandler extends AbstractParseCharBufferHandler<
        DoubleParserResult,
        DoubleParsingStrategy> {

    public StringToDoubleParseCharBufferHandler(DoubleParsingStrategy parsingStrategy,
                                                int[] myColumns) {
        super(parsingStrategy, myColumns, ColumnType.DOUBLE);
    }

    @Override
    protected void finished(TableRow tableRow) {
        tableRow.setDoublesIsFinished();
    }

    @Override
    protected void setValue(TableRow tableRow, int index, DoubleParserResult result) {
        tableRow.setDouble(result.value, index, result.state);
    }
}
