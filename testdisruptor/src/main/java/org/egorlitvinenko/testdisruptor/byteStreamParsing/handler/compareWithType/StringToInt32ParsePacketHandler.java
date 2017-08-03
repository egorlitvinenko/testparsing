package org.egorlitvinenko.testdisruptor.byteStreamParsing.handler.compareWithType;

import org.egorlitvinenko.testdisruptor.byteStreamParsing.model.TableRow;
import org.egorlitvinenko.testdisruptor.byteStreamParsing.parsing.api.Int32ParserResult;
import org.egorlitvinenko.testdisruptor.byteStreamParsing.parsing.api.Int32ParsingStrategy;
import org.egorlitvinenko.testdisruptor.byteStreamParsing.util.ColumnType;

/**
 * @author Egor Litvinenko
 */
public class StringToInt32ParsePacketHandler extends AbstractParsePacketHandler<
        Int32ParserResult,
        Int32ParsingStrategy> {

    public StringToInt32ParsePacketHandler(Int32ParsingStrategy parsingStrategy,
                                           int[] myColumns) {
        super(parsingStrategy, myColumns, ColumnType.INT_32);
    }

    @Override
    protected void finished(TableRow tableRow) {
        tableRow.setInt32IsFinished();
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
