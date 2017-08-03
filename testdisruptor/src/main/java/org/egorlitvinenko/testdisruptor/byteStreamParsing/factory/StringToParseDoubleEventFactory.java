package org.egorlitvinenko.testdisruptor.byteStreamParsing.factory;

import org.egorlitvinenko.testdisruptor.byteStreamParsing.event.StringToParseDoubleEvent;
import com.lmax.disruptor.EventFactory;

/**
 * @author Egor Litvinenko
 */
public class StringToParseDoubleEventFactory implements EventFactory<StringToParseDoubleEvent> {

    @Override
    public StringToParseDoubleEvent newInstance() {
        return new StringToParseDoubleEvent();
    }
}
