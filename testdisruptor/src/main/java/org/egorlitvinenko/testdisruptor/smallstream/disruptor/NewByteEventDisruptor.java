package org.egorlitvinenko.testdisruptor.smallstream.disruptor;

import org.egorlitvinenko.testdisruptor.smallstream.handler.CharacterAggregatorFromQuotedCsv;
import org.egorlitvinenko.testdisruptor.smallstream.ByteStream5StringAggregationMain;
import org.egorlitvinenko.testdisruptor.smallstream.event.NewByteEvent;
import org.egorlitvinenko.testdisruptor.smallstream.event.NewStringEvent;
import org.egorlitvinenko.testdisruptor.smallstream.factory.NewCharacterEventFactory;
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
public class NewByteEventDisruptor extends AbstractDisruptorFactory<NewByteEvent> {

    public Disruptor<NewByteEvent> createForStringAggregator(ThreadFactory threadFactory, RingBuffer<NewStringEvent> stringBuffer) {
        return create(threadFactory,
                new CharacterAggregatorFromQuotedCsv((byte) '\n', (byte) ',', (byte) '"', stringBuffer));
    }

    @Override
    protected EventFactory<NewByteEvent> createEventFactory() {
        return new NewCharacterEventFactory();
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
        return ByteStream5StringAggregationMain.NEW_BYTE_BUFFER;
    }
}
