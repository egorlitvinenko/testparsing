package org.egorlitvinenko.testdisruptor.byteStreamParsing.handler.splittedByColumn;

import org.egorlitvinenko.testdisruptor.byteStreamParsing.model.TableRow;
import org.egorlitvinenko.testdisruptor.byteStreamParsing.parsing.api.LocalDateParserResult;
import org.egorlitvinenko.testdisruptor.byteStreamParsing.parsing.api.LocalDateParserStrategy;

/**
 * @author Egor Litvinenko
 */
public class StringToLocalDateParseHandler2 extends AbstractParseHandler2<
        LocalDateParserResult,
        LocalDateParserStrategy> {

    public StringToLocalDateParseHandler2(LocalDateParserStrategy parsingStrategy,
                                          boolean[] myColumns) {
        super(parsingStrategy, myColumns);
    }

    @Override
    protected String getStringValue(TableRow tableRow, int index) {
        return tableRow.getLocalDateString(index);
    }

    @Override
    protected void setValue(TableRow tableRow, int index, LocalDateParserResult result) {
        tableRow.setLocalDate(result.value, index, result.state);
    }
}
