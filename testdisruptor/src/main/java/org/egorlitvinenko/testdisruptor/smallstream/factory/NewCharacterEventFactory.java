package org.egorlitvinenko.testdisruptor.smallstream.factory;

import org.egorlitvinenko.testdisruptor.smallstream.event.NewByteEvent;
import com.lmax.disruptor.EventFactory;

/**
 * @author Egor Litvinenko
 */
public class NewCharacterEventFactory implements EventFactory<NewByteEvent> {

    @Override
    public NewByteEvent newInstance() {
        return new NewByteEvent();
    }
}
