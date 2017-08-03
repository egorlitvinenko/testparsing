package org.egorlitvinenko.testdisruptor.byteStreamParsing.handler.splittedByColumn;

import org.egorlitvinenko.testdisruptor.byteStreamParsing.model.TableRow;
import org.egorlitvinenko.testdisruptor.byteStreamParsing.parsing.api.DoubleParserResult;
import org.egorlitvinenko.testdisruptor.byteStreamParsing.parsing.api.DoubleParsingStrategy;

/**
 * @author Egor Litvinenko
 */
public class StringToDoubleParseHandler2 extends AbstractParseHandler2<
        DoubleParserResult,
        DoubleParsingStrategy> {

    public StringToDoubleParseHandler2(DoubleParsingStrategy parsingStrategy,
                                       boolean[] myColumns) {
        super(parsingStrategy, myColumns);
    }

    @Override
    protected String getStringValue(TableRow tableRow, int index) {
        return tableRow.getDoubleString(index);
    }

    @Override
    protected void setValue(TableRow tableRow, int index, DoubleParserResult result) {
        tableRow.setDouble(result.value, index, result.state);
    }
}
