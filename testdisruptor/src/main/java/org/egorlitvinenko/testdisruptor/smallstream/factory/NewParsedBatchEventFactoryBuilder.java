package org.egorlitvinenko.testdisruptor.smallstream.factory;

import org.egorlitvinenko.testdisruptor.smallstream.event.NewParsedBatchEvent;
import com.lmax.disruptor.EventFactory;

/**
 * @author Egor Litvinenko
 */
public class NewParsedBatchEventFactoryBuilder  {

    private int batchSize;
    private int columnCount;

    public NewParsedBatchEventFactoryBuilder batchSize(int batchSize) {
        this.batchSize = batchSize;
        return this;
    }

    public NewParsedBatchEventFactoryBuilder columnCount(int columnCount) {
        this.columnCount = columnCount;
        return this;
    }

    public EventFactory<NewParsedBatchEvent> createFactory() {
        return new EventFactory<NewParsedBatchEvent>() {
            @Override
            public NewParsedBatchEvent newInstance() {
                return new NewParsedBatchEvent();
            }
        };
    }


}
