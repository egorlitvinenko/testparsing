package org.egorlitvinenko.testdisruptor.smallstream.disruptor;

import org.egorlitvinenko.testdisruptor.Clickhouse;
import org.egorlitvinenko.testdisruptor.smallstream.event.NewParsedBatchEvent;
import org.egorlitvinenko.testdisruptor.smallstream.handler.BatchParser;
import org.egorlitvinenko.testdisruptor.smallstream.handler.ClickhouseStringBatchSaver;
import org.egorlitvinenko.testdisruptor.smallstream.event.NewBatchEvent;
import org.egorlitvinenko.testdisruptor.smallstream.factory.NewBatchEventFactory;
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
public class NewBatchEventDisruptor extends AbstractDisruptorFactory<NewBatchEvent> {

    public Disruptor<NewBatchEvent> createForClickhouseBatchSaver(ThreadFactory threadFactory) {
        return create(threadFactory, new ClickhouseStringBatchSaver(Clickhouse.INSERT_5_STRINGS));
    }

    public Disruptor<NewBatchEvent> createForParsing(ThreadFactory threadFactory,
                                                     byte[] types,
                                                     RingBuffer<NewParsedBatchEvent> newParsedBatchEventRingBuffer) {
        return create(threadFactory, new BatchParser(types, newParsedBatchEventRingBuffer));
    }

    @Override
    protected EventFactory<NewBatchEvent> createEventFactory() {
        return new NewBatchEventFactory();
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
        return 1024 * 4;
    }
}
