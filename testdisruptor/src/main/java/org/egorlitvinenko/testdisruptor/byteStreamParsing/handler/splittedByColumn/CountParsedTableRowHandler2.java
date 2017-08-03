package org.egorlitvinenko.testdisruptor.byteStreamParsing.handler.splittedByColumn;

import org.egorlitvinenko.testdisruptor.byteStreamParsing.event.ParseTableRowEvent;
import com.lmax.disruptor.EventHandler;

/**
 * @author Egor Litvinenko
 */
public class CountParsedTableRowHandler2 implements EventHandler<ParseTableRowEvent> {

    private int counter = 0;
    private int calls = 0;

    @Override
    public void onEvent(ParseTableRowEvent parsedTableRowEvent, long l, boolean b) throws Exception {
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
