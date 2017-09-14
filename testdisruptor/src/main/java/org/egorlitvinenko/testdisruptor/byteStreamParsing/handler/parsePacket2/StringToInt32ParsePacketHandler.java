package org.egorlitvinenko.testdisruptor.byteStreamParsing.handler.parsePacket2;

import org.egorlitvinenko.testdisruptor.byteStreamParsing.event.ParsePacketEvent;
import org.egorlitvinenko.testdisruptor.byteStreamParsing.model.TableRow;
import org.egorlitvinenko.testdisruptor.byteStreamParsing.parsing3.SimpleParseInt;

/**
 * @author Egor Litvinenko
 */
public class StringToInt32ParsePacketHandler extends AbstractParsePacketHandler {

    public StringToInt32ParsePacketHandler(int[] myColumns) {
        super(new SimpleParseInt(), myColumns);
    }

    @Override
    protected boolean hasMyType(ParsePacketEvent event) {
        return event.hasInt32();
    }

    @Override
    protected void finished(TableRow tableRow) {
        tableRow.setInt32IsFinished();
    }

}
