package org.egorlitvinenko.testdisruptor.byteStreamParsing.handler.splittedByEvent;

import org.egorlitvinenko.testdisruptor.byteStreamParsing.event.ParsedTableRowEvent;
import org.egorlitvinenko.testdisruptor.byteStreamParsing.event.StringToParseInt32Event;
import org.egorlitvinenko.testdisruptor.byteStreamParsing.model.TableRow;
import org.egorlitvinenko.testdisruptor.byteStreamParsing.parsing.api.Int32ParserResult;
import org.egorlitvinenko.testdisruptor.byteStreamParsing.parsing.api.Int32ParsingStrategy;
import com.lmax.disruptor.RingBuffer;

/**
 * @author Egor Litvinenko
 */
public class StringToInt32ParseHandler extends AbstractParseHandler<
        StringToParseInt32Event,
        Int32ParserResult,
        Int32ParsingStrategy> {

    public StringToInt32ParseHandler(Int32ParsingStrategy parsingStrategy,
                                     RingBuffer<ParsedTableRowEvent> ringBuffer) {
        super(parsingStrategy, ringBuffer);
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
