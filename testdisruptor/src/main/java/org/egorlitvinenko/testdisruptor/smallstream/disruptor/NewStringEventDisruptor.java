package org.egorlitvinenko.testdisruptor.smallstream.disruptor;

import org.egorlitvinenko.testdisruptor.smallstream.handler.LineAggregator;
import org.egorlitvinenko.testdisruptor.smallstream.ByteStream5StringAggregationMain;
import org.egorlitvinenko.testdisruptor.smallstream.event.NewCombinedStringLineEvent;
import org.egorlitvinenko.testdisruptor.smallstream.event.NewStringEvent;
import org.egorlitvinenko.testdisruptor.smallstream.factory.NewStringEventFactory;
import com.lmax.disruptor.EventFactory;
import com.lmax.disruptor.RingBuffer;
import com.lmax.disruptor.WaitStrategy;
import com.lmax.disruptor.YieldingWaitStrategy;
import com.lmax.disruptor.dsl.Disruptor;
import com.lmax.disruptor.dsl.ProducerType;

import java.util.concurrent.ThreadFactory;

/**
 * @author Egor Litvinenko
 */
public class NewStringEventDisruptor extends AbstractDisruptorFactory<NewStringEvent> {

    public Disruptor<NewStringEvent> createForLineAggregator(int lineSize, ThreadFactory threadFactory, RingBuffer<NewCombinedStringLineEvent> combinedStringLineEventRingBuffer) {
        return create(threadFactory, new LineAggregator(lineSize, combinedStringLineEventRingBuffer));
    }

    @Override
    protected EventFactory<NewStringEvent> createEventFactory() {
        return new NewStringEventFactory();
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
        return ByteStream5StringAggregationMain.NEW_STRING_BUFFER;
    }
}
