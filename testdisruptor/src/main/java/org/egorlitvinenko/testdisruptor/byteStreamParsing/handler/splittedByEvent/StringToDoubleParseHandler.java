package org.egorlitvinenko.testdisruptor.byteStreamParsing.handler.splittedByEvent;

import org.egorlitvinenko.testdisruptor.byteStreamParsing.event.ParsedTableRowEvent;
import org.egorlitvinenko.testdisruptor.byteStreamParsing.event.StringToParseDoubleEvent;
import org.egorlitvinenko.testdisruptor.byteStreamParsing.model.TableRow;
import org.egorlitvinenko.testdisruptor.byteStreamParsing.parsing.api.DoubleParserResult;
import org.egorlitvinenko.testdisruptor.byteStreamParsing.parsing.api.DoubleParsingStrategy;
import com.lmax.disruptor.RingBuffer;

/**
 * @author Egor Litvinenko
 */
public class StringToDoubleParseHandler extends AbstractParseHandler<
        StringToParseDoubleEvent,
        DoubleParserResult,
        DoubleParsingStrategy> {

    public StringToDoubleParseHandler(DoubleParsingStrategy parsingStrategy,
            RingBuffer<ParsedTableRowEvent> ringBuffer) {
        super(parsingStrategy, ringBuffer);
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
