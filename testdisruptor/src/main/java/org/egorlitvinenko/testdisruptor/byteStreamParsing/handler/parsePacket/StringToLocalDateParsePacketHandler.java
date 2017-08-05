package org.egorlitvinenko.testdisruptor.byteStreamParsing.handler.parsePacket;

import org.egorlitvinenko.testdisruptor.byteStreamParsing.event.ParsePacketEvent;
import org.egorlitvinenko.testdisruptor.byteStreamParsing.model.TableRow;
import org.egorlitvinenko.testdisruptor.byteStreamParsing.parsing.api.LocalDateParserResult;
import org.egorlitvinenko.testdisruptor.byteStreamParsing.parsing.api.LocalDateParserStrategy;
import org.egorlitvinenko.testdisruptor.byteStreamParsing.util.ColumnType;

/**
 * @author Egor Litvinenko
 */
public class StringToLocalDateParsePacketHandler extends AbstractParsePacketHandler<
        LocalDateParserResult,
        LocalDateParserStrategy> {

    public StringToLocalDateParsePacketHandler(LocalDateParserStrategy parsingStrategy,
                                               int[] myColumns) {
        super(parsingStrategy, myColumns);
    }

    @Override
    protected boolean hasMyType(ParsePacketEvent event) {
        return event.hasLocalDates();
    }

    @Override
    protected void finished(TableRow tableRow) {
        tableRow.setLocalDateIsFinished();
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
