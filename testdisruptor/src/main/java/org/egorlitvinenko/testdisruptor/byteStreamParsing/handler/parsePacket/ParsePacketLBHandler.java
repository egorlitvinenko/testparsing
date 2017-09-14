package org.egorlitvinenko.testdisruptor.byteStreamParsing.handler.parsePacket;

import com.lmax.disruptor.EventHandler;
import com.lmax.disruptor.EventTranslatorOneArg;
import com.lmax.disruptor.RingBuffer;
import org.egorlitvinenko.testdisruptor.byteStreamParsing.event.ParsePacketTableRowEvent;
import org.egorlitvinenko.testdisruptor.byteStreamParsing.model.TableRow;

/**
 * @author Egor Litvinenko
 */
public class ParsePacketLBHandler implements EventHandler<ParsePacketTableRowEvent> {

    private final RingBuffer<ParsePacketTableRowEvent>[] listeners;

    private int listenersCounter = 0;

    public ParsePacketLBHandler(RingBuffer<ParsePacketTableRowEvent>[] listeners) {
        this.listeners = listeners;
    }

    protected RingBuffer<ParsePacketTableRowEvent> getListener(TableRow tableRow) {
        int scope;
        if (-1 == tableRow.getScope()) {
            listenersCounter = (listenersCounter + 1) & listeners.length;
            scope = tableRow.suggestScopeAndGet(listenersCounter);
        } else {
            scope = tableRow.getScope();
        }
        return listeners[scope];
    }

    @Override
    public void onEvent(ParsePacketTableRowEvent event, long sequence, boolean endOfBatch) throws Exception {
        if (event.isEnd()) {
            for (RingBuffer<ParsePacketTableRowEvent> rb : listeners) {
                rb.publishEvent(TRANSLATOR, event);
            }
        } else {
            getListener(event.getTableRow()).publishEvent(TRANSLATOR, event);
        }
    }

    private static final EventTranslatorOneArg<ParsePacketTableRowEvent, ParsePacketTableRowEvent> TRANSLATOR =
            new EventTranslatorOneArg<ParsePacketTableRowEvent, ParsePacketTableRowEvent>() {
        @Override
        public void translateTo(ParsePacketTableRowEvent event, long sequence, ParsePacketTableRowEvent arg0) {
            event.setEnd(arg0.isEnd());
            event.setTableRow(arg0.getTableRow());
            event.setType(arg0.getType());
        }
    };

}
