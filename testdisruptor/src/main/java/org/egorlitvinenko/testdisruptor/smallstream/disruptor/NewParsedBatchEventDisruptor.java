package org.egorlitvinenko.testdisruptor.smallstream.disruptor;

import org.egorlitvinenko.testdisruptor.smallstream.event.NewParsedBatchEvent;
import org.egorlitvinenko.testdisruptor.smallstream.factory.NewParsedBatchEventFactory;
import org.egorlitvinenko.testdisruptor.smallstream.handler.ClickhouseTypedBatchSaver;
import com.lmax.disruptor.EventFactory;
import com.lmax.disruptor.WaitStrategy;
import com.lmax.disruptor.YieldingWaitStrategy;
import com.lmax.disruptor.dsl.Disruptor;
import com.lmax.disruptor.dsl.ProducerType;

import java.util.concurrent.ThreadFactory;

/**
 * @author Egor Litvinenko
 */
public class NewParsedBatchEventDisruptor extends AbstractDisruptorFactory<NewParsedBatchEvent> {

    public Disruptor<NewParsedBatchEvent> createForClickhouseTypedBatchSaver(ThreadFactory threadFactory,
                                                                             String insert) {
        return create(threadFactory, new ClickhouseTypedBatchSaver(insert));
    }

    @Override
    protected EventFactory<NewParsedBatchEvent> createEventFactory() {
        return new NewParsedBatchEventFactory();
    }

    @Override
    protected WaitStrategy getWaitStrategy() {
        return new YieldingWaitStrategy();
    }

    @Override
    protected ProducerType getProducerType() {
        return ProducerType.SINGLE;
    }

    @Override
    protected int getRingBufferSize() {
        return 1024;
    }
}
