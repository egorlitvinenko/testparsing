package org.egorlitvinenko.testdisruptor.byteStreamParsing.factory;

import org.egorlitvinenko.testdisruptor.byteStreamParsing.event.ByteInLineEvent;
import com.lmax.disruptor.EventFactory;

/**
 * @author Egor Litvinenko
 */
public class ByteInLineEventFactory implements EventFactory<ByteInLineEvent> {

    @Override
    public ByteInLineEvent newInstance() {
        return new ByteInLineEvent();
    }
}
