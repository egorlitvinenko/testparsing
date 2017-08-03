package org.egorlitvinenko.testdisruptor.smallstream.disruptor;

import org.egorlitvinenko.testdisruptor.Clickhouse;
import org.egorlitvinenko.testdisruptor.smallstream.handler.ClickhouseStringLineSaver;
import org.egorlitvinenko.testdisruptor.smallstream.ByteStream5StringAggregationMain;
import org.egorlitvinenko.testdisruptor.smallstream.event.NewCombinedStringLineEvent;
import org.egorlitvinenko.testdisruptor.smallstream.factory.NewCombinedStringLineEventFactory;
import com.lmax.disruptor.EventFactory;
import com.lmax.disruptor.WaitStrategy;
import com.lmax.disruptor.YieldingWaitStrategy;
import com.lmax.disruptor.dsl.Disruptor;
import com.lmax.disruptor.dsl.ProducerType;

import java.util.concurrent.ThreadFactory;

/**
 * @author Egor Litvinenko
 */
public class NewCombinedStringLineDisruptor extends AbstractDisruptorFactory<NewCombinedStringLineEvent> {

    public Disruptor<NewCombinedStringLineEvent> createForClickhouseLineSaver(ThreadFactory threadFactory) {
        return create(threadFactory, new ClickhouseStringLineSaver(Clickhouse.INSERT_5_STRINGS));
    }

    @Override
    protected EventFactory<NewCombinedStringLineEvent> createEventFactory() {
        return new NewCombinedStringLineEventFactory();
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
        return ByteStream5StringAggregationMain.NEW_LINE_BUFFER;
    }
}
