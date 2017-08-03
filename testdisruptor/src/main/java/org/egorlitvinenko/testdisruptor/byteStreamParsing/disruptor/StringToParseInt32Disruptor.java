package org.egorlitvinenko.testdisruptor.byteStreamParsing.disruptor;

import org.egorlitvinenko.testdisruptor.byteStreamParsing.ByteStreamParsingConstants;
import org.egorlitvinenko.testdisruptor.byteStreamParsing.event.StringToParseInt32Event;
import org.egorlitvinenko.testdisruptor.byteStreamParsing.factory.StringToParseInt32EventFactory;
import org.egorlitvinenko.testdisruptor.smallstream.disruptor.AbstractDisruptorFactory;
import com.lmax.disruptor.EventFactory;
import com.lmax.disruptor.SleepingWaitStrategy;
import com.lmax.disruptor.WaitStrategy;
import com.lmax.disruptor.dsl.ProducerType;

/**
 * @author Egor Litvinenko
 */
public class StringToParseInt32Disruptor extends AbstractDisruptorFactory<StringToParseInt32Event> {

    @Override
    protected EventFactory<StringToParseInt32Event> createEventFactory() {
        return new StringToParseInt32EventFactory();
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
        return ByteStreamParsingConstants.STRING_TO_PARSE_INT_32_BUFFER;
    }
}
