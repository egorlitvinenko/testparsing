package org.egorlitvinenko.testdisruptor.smallstream.factory;

import org.egorlitvinenko.testdisruptor.smallstream.event.NewCombinedStringLineEvent;
import com.lmax.disruptor.EventFactory;

/**
 * @author Egor Litvinenko
 */
public class NewCombinedStringLineEventFactory implements EventFactory<NewCombinedStringLineEvent> {

    @Override
    public NewCombinedStringLineEvent newInstance() {
        return new NewCombinedStringLineEvent();
    }
}
