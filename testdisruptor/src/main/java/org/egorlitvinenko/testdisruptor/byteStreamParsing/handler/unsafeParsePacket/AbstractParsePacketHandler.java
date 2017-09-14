package org.egorlitvinenko.testdisruptor.byteStreamParsing.handler.unsafeParsePacket;

import com.lmax.disruptor.EventHandler;
import org.egorlitvinenko.testdisruptor.byteStreamParsing.event.UnsafeParsePacketEvent;
import org.egorlitvinenko.testdisruptor.byteStreamParsing.model.TableRow;
import org.egorlitvinenko.testdisruptor.byteStreamParsing.parsing2.InternalParsingStrategy;

/**
 * @author Egor Litvinenko
 */
public abstract class AbstractParsePacketHandler implements EventHandler<UnsafeParsePacketEvent> {

    protected final int[] myColumns;
    protected final InternalParsingStrategy parser;

    public AbstractParsePacketHandler(InternalParsingStrategy parser, int[] myColumns) {
        this.myColumns = myColumns;
        this.parser = parser;
    }

    @Override
    public void onEvent(UnsafeParsePacketEvent event, long sequence, boolean endOfBatch) throws Exception {
        if (!event.isEnd() && hasMyType(event)) {
            for (int i = 0; i < myColumns.length; ++i) {
                parser.parse(event, myColumns[i]);
            }
            finished(event);
        }
    }

    protected abstract boolean hasMyType(UnsafeParsePacketEvent event);

    protected abstract void finished(TableRow tableRow);

}
