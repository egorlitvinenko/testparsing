package org.egorlitvinenko.testdisruptor.byteStreamParsing.handler.parsePacket2;

import com.lmax.disruptor.EventHandler;
import org.egorlitvinenko.testdisruptor.byteStreamParsing.event.ParsePacketEvent;
import org.egorlitvinenko.testdisruptor.byteStreamParsing.model.TableRow;
import org.egorlitvinenko.testdisruptor.byteStreamParsing.parsing3.TableRowInternalParsingStrategy;

/**
 * @author Egor Litvinenko
 */
public abstract class AbstractParsePacketHandler implements EventHandler<ParsePacketEvent> {

    protected final int[] myColumns;
    protected final TableRowInternalParsingStrategy parser;

    public AbstractParsePacketHandler(TableRowInternalParsingStrategy parser, int[] myColumns) {
        this.myColumns = myColumns;
        this.parser = parser;
    }

    @Override
    public void onEvent(ParsePacketEvent event, long sequence, boolean endOfBatch) throws Exception {
        if (!event.isEnd() && hasMyType(event)) {
            for (int i = 0; i < myColumns.length; ++i) {
                parser.parse(event, myColumns[i]);
            }
            finished(event);
        }
    }

    protected abstract boolean hasMyType(ParsePacketEvent event);

    protected abstract void finished(TableRow tableRow);

}
