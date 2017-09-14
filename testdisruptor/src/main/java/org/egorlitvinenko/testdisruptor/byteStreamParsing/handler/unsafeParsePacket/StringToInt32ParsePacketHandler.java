package org.egorlitvinenko.testdisruptor.byteStreamParsing.handler.unsafeParsePacket;

import org.egorlitvinenko.testdisruptor.byteStreamParsing.event.UnsafeParsePacketEvent;
import org.egorlitvinenko.testdisruptor.byteStreamParsing.model.TableRow;
import org.egorlitvinenko.testdisruptor.byteStreamParsing.parsing2.SimpleParseInt;

/**
 * @author Egor Litvinenko
 */
public class StringToInt32ParsePacketHandler extends AbstractParsePacketHandler {

    public StringToInt32ParsePacketHandler(int[] myColumns) {
        super(new SimpleParseInt(), myColumns);
    }

    @Override
    protected boolean hasMyType(UnsafeParsePacketEvent event) {
        return event.hasInt32();
    }

    @Override
    protected void finished(TableRow tableRow) {
        tableRow.setInt32IsFinished();
    }
}
