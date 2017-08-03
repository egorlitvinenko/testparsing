package org.egorlitvinenko.testdisruptor.smallstream.factory;

import org.egorlitvinenko.testdisruptor.smallstream.event.NewStringEvent;
import com.lmax.disruptor.EventFactory;

/**
 * @author Egor Litvinenko
 */
public class NewStringEventFactory implements EventFactory<NewStringEvent> {
    @Override
    public NewStringEvent newInstance() {
        return new NewStringEvent();
    }
}
