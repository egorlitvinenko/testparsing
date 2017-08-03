package org.egorlitvinenko.testdisruptor.byteStreamParsing.factory;

import org.egorlitvinenko.testdisruptor.byteStreamParsing.event.ParsedTableRowEvent;
import com.lmax.disruptor.EventFactory;

/**
 * @author Egor Litvinenko
 */
public class ParsedTableRowEventFactory implements EventFactory<ParsedTableRowEvent> {

    @Override
    public ParsedTableRowEvent newInstance() {
        return new ParsedTableRowEvent();
    }
}
