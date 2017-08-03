package org.egorlitvinenko.testdisruptor.byteStreamParsing.factory;

import org.egorlitvinenko.testdisruptor.byteStreamParsing.event.StringToParseInt32Event;
import com.lmax.disruptor.EventFactory;

/**
 * @author Egor Litvinenko
 */
public class StringToParseInt32EventFactory implements EventFactory<StringToParseInt32Event> {

    @Override
    public StringToParseInt32Event newInstance() {
        return new StringToParseInt32Event();
    }
}
