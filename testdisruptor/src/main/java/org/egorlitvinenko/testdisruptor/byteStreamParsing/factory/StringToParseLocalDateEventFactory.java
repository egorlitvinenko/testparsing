package org.egorlitvinenko.testdisruptor.byteStreamParsing.factory;

import org.egorlitvinenko.testdisruptor.byteStreamParsing.event.StringToParseLocalDateEvent;
import com.lmax.disruptor.EventFactory;

/**
 * @author Egor Litvinenko
 */
public class StringToParseLocalDateEventFactory implements EventFactory<StringToParseLocalDateEvent> {

    @Override
    public StringToParseLocalDateEvent newInstance() {
        return new StringToParseLocalDateEvent();
    }
}
