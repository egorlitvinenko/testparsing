package org.egorlitvinenko.testdisruptor.byteStreamParsing.handler.parsePacket2;

import org.egorlitvinenko.testdisruptor.byteStreamParsing.event.ParsePacketEvent;
import org.egorlitvinenko.testdisruptor.byteStreamParsing.model.TableRow;
import org.egorlitvinenko.testdisruptor.byteStreamParsing.parsing3.SimpleParseDouble;

/**
 * @author Egor Litvinenko
 */
public class StringToDoubleParsePacketHandler extends AbstractParsePacketHandler {

    public StringToDoubleParsePacketHandler(int[] myColumns) {
        super(new SimpleParseDouble(), myColumns);
    }

    @Override
    protected boolean hasMyType(ParsePacketEvent event) {
        return event.hasDoubles();
    }

    @Override
    protected void finished(TableRow tableRow) {
        tableRow.setDoublesIsFinished();
    }

}
