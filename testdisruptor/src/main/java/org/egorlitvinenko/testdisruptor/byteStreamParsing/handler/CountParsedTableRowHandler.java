package org.egorlitvinenko.testdisruptor.byteStreamParsing.handler;

import org.egorlitvinenko.testdisruptor.byteStreamParsing.event.ParsedTableRowEvent;
import com.lmax.disruptor.EventHandler;

/**
 * @author Egor Litvinenko
 */
public class CountParsedTableRowHandler implements EventHandler<ParsedTableRowEvent> {

    private int counter = 0;
    private int calls = 0;

    @Override
    public void onEvent(ParsedTableRowEvent parsedTableRowEvent, long l, boolean b) throws Exception {
        ++calls;
        parsedTableRowEvent.getTableRow().incrementProcessedElements();
        if (parsedTableRowEvent.getTableRow().rowIsProcessed()) {
            counter++;
            parsedTableRowEvent.getTableRow().close();
        }
    }

    public int getCounter() {
        return counter;
    }

    public int getCalls() {
        return calls;
    }
}
