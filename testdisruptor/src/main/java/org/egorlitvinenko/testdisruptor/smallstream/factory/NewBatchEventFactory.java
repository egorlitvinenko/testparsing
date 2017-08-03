package org.egorlitvinenko.testdisruptor.smallstream.factory;

import org.egorlitvinenko.testdisruptor.smallstream.event.NewBatchEvent;
import com.lmax.disruptor.EventFactory;

/**
 * @author Egor Litvinenko
 */
public class NewBatchEventFactory implements EventFactory<NewBatchEvent> {

    @Override
    public NewBatchEvent newInstance() {
        return new NewBatchEvent();
    }
}
