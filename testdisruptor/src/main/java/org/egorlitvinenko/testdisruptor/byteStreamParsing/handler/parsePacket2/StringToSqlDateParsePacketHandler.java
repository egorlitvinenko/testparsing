package org.egorlitvinenko.testdisruptor.byteStreamParsing.handler.parsePacket2;

import org.egorlitvinenko.testdisruptor.byteStreamParsing.event.ParsePacketEvent;
import org.egorlitvinenko.testdisruptor.byteStreamParsing.model.TableRow;
import org.egorlitvinenko.testdisruptor.byteStreamParsing.parsing3.SqlDateParser;

/**
 * @author Egor Litvinenko
 */
public class StringToSqlDateParsePacketHandler extends AbstractParsePacketHandler {

    public StringToSqlDateParsePacketHandler(int[] myColumns) {
        super(new SqlDateParser(), myColumns);
    }

    @Override
    protected boolean hasMyType(ParsePacketEvent event) {
        return event.hasSqlDates();
    }

    @Override
    protected void finished(TableRow tableRow) {
        tableRow.setSqlDateIsFinished();
    }

}
