package org.egorlitvinenko.testdisruptor.byteStreamParsing.disruptor;

import org.egorlitvinenko.testdisruptor.byteStreamParsing.ByteStreamParsingConstants;
import org.egorlitvinenko.testdisruptor.byteStreamParsing.event.ByteInLineEvent;
import org.egorlitvinenko.testdisruptor.byteStreamParsing.factory.ByteInLineEventFactory;
import org.egorlitvinenko.testdisruptor.smallstream.disruptor.AbstractDisruptorFactory;
import com.lmax.disruptor.BlockingWaitStrategy;
import com.lmax.disruptor.EventFactory;
import com.lmax.disruptor.WaitStrategy;
import com.lmax.disruptor.dsl.ProducerType;

/**
 * @author Egor Litvinenko
 */
@Deprecated
public class ByteInLineDisruptor extends AbstractDisruptorFactory<ByteInLineEvent> {

    @Override
    protected EventFactory<ByteInLineEvent> createEventFactory() {
        return new ByteInLineEventFactory();
    }

    @Override
    protected WaitStrategy getWaitStrategy() {
        return new BlockingWaitStrategy();
    }

    @Override
    protected ProducerType getProducerType() {
        return ProducerType.SINGLE;
    }

    @Override
    protected int getRingBufferSize() {
        return ByteStreamParsingConstants.BYTE_IN_LINE_BUFFER;
    }

}
