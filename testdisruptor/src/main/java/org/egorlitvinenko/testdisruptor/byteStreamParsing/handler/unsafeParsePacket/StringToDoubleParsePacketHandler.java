package org.egorlitvinenko.testdisruptor.byteStreamParsing.handler.unsafeParsePacket;

import org.egorlitvinenko.testdisruptor.byteStreamParsing.event.UnsafeParsePacketEvent;
import org.egorlitvinenko.testdisruptor.byteStreamParsing.model.TableRow;
import org.egorlitvinenko.testdisruptor.byteStreamParsing.parsing2.SimpleParseDouble;

/**
 * @author Egor Litvinenko
 */
public class StringToDoubleParsePacketHandler extends AbstractParsePacketHandler {

    public StringToDoubleParsePacketHandler(int[] myColumns) {
        super(new SimpleParseDouble(), myColumns);
    }

    @Override
    protected boolean hasMyType(UnsafeParsePacketEvent event) {
        return event.hasDoubles();
    }

    @Override
    protected void finished(TableRow tableRow) {
        tableRow.setDoublesIsFinished();
    }
}
