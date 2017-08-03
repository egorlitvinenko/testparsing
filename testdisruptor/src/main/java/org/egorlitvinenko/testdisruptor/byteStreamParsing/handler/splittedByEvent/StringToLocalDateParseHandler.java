package org.egorlitvinenko.testdisruptor.byteStreamParsing.handler.splittedByEvent;

import org.egorlitvinenko.testdisruptor.byteStreamParsing.event.ParsedTableRowEvent;
import org.egorlitvinenko.testdisruptor.byteStreamParsing.event.StringToParseLocalDateEvent;
import org.egorlitvinenko.testdisruptor.byteStreamParsing.model.TableRow;
import org.egorlitvinenko.testdisruptor.byteStreamParsing.parsing.api.LocalDateParserResult;
import org.egorlitvinenko.testdisruptor.byteStreamParsing.parsing.api.LocalDateParserStrategy;
import com.lmax.disruptor.RingBuffer;

/**
 * @author Egor Litvinenko
 */
public class StringToLocalDateParseHandler extends AbstractParseHandler<
        StringToParseLocalDateEvent,
        LocalDateParserResult,
        LocalDateParserStrategy> {

    public StringToLocalDateParseHandler(LocalDateParserStrategy parsingStrategy,
                                         RingBuffer<ParsedTableRowEvent> ringBuffer) {
        super(parsingStrategy, ringBuffer);
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
