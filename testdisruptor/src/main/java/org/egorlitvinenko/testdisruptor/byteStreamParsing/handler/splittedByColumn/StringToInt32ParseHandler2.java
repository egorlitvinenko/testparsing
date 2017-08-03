package org.egorlitvinenko.testdisruptor.byteStreamParsing.handler.splittedByColumn;

import org.egorlitvinenko.testdisruptor.byteStreamParsing.model.TableRow;
import org.egorlitvinenko.testdisruptor.byteStreamParsing.parsing.api.Int32ParserResult;
import org.egorlitvinenko.testdisruptor.byteStreamParsing.parsing.api.Int32ParsingStrategy;

/**
 * @author Egor Litvinenko
 */
public class StringToInt32ParseHandler2 extends AbstractParseHandler2<
        Int32ParserResult,
        Int32ParsingStrategy> {

    public StringToInt32ParseHandler2(Int32ParsingStrategy parsingStrategy,
                                      boolean[] myColumns) {
        super(parsingStrategy, myColumns);
    }

    @Override
    protected String getStringValue(TableRow tableRow, int index) {
        return tableRow.getInt32String(index);
    }

    @Override
    protected void setValue(TableRow tableRow, int index, Int32ParserResult result) {
        tableRow.setInt32(result.value, index, result.state);
    }
}
