package org.egorlitvinenko.testdisruptor.smallstream.factory;

import org.egorlitvinenko.testdisruptor.smallstream.event.NewParsedBatchEvent;
import com.lmax.disruptor.EventFactory;

/**
 * @author Egor Litvinenko
 */
public class NewParsedBatchEventFactory implements EventFactory<NewParsedBatchEvent> {

    public NewParsedBatchEvent newInstance() {
        return new NewParsedBatchEvent();
    }

}
