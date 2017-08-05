package org.egorlitvinenko.testdisruptor.byteStreamParsing.disruptor;

import com.lmax.disruptor.EventFactory;
import com.lmax.disruptor.SleepingWaitStrategy;
import com.lmax.disruptor.WaitStrategy;
import com.lmax.disruptor.dsl.ProducerType;
import org.egorlitvinenko.testdisruptor.byteStreamParsing.event.ReadFileToCharBufferEvent;
import org.egorlitvinenko.testdisruptor.smallstream.disruptor.AbstractDisruptorFactory;

/**
 * @author Egor Litvinenko
 */
public class ReadCharBufferDisruptor extends AbstractDisruptorFactory<ReadFileToCharBufferEvent> {

    @Override
    protected EventFactory<ReadFileToCharBufferEvent> createEventFactory() {
        return new EventFactory<ReadFileToCharBufferEvent>() {
            @Override
            public ReadFileToCharBufferEvent newInstance() {
                return new ReadFileToCharBufferEvent();
            }
        };
    }

    @Override
    protected WaitStrategy getWaitStrategy() {
        return new SleepingWaitStrategy();
    }

    @Override
    protected ProducerType getProducerType() {
        return ProducerType.SINGLE;
    }

    @Override
    protected int getRingBufferSize() {
        return 1024*8;
    }
}
