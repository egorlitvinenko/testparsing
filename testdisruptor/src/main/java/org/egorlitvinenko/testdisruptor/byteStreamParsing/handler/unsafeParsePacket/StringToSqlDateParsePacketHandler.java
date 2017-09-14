package org.egorlitvinenko.testdisruptor.byteStreamParsing.handler.unsafeParsePacket;

import org.egorlitvinenko.testdisruptor.byteStreamParsing.event.UnsafeParsePacketEvent;
import org.egorlitvinenko.testdisruptor.byteStreamParsing.model.TableRow;
import org.egorlitvinenko.testdisruptor.byteStreamParsing.parsing2.SqlDateParser;

/**
 * @author Egor Litvinenko
 */
public class StringToSqlDateParsePacketHandler extends AbstractParsePacketHandler {

    public StringToSqlDateParsePacketHandler(int[] myColumns) {
        super(new SqlDateParser(), myColumns);
    }

    @Override
    protected boolean hasMyType(UnsafeParsePacketEvent event) {
        return event.hasSqlDates();
    }

    @Override
    protected void finished(TableRow tableRow) {
        tableRow.setSqlDateIsFinished();
    }
}
