package org.egorlitvinenko.testdisruptor.byteStreamParsing.disruptor;

import org.egorlitvinenko.testdisruptor.byteStreamParsing.ByteStreamParsingConstants;
import org.egorlitvinenko.testdisruptor.byteStreamParsing.event.StringToParseDoubleEvent;
import org.egorlitvinenko.testdisruptor.byteStreamParsing.factory.StringToParseDoubleEventFactory;
import org.egorlitvinenko.testdisruptor.smallstream.disruptor.AbstractDisruptorFactory;
import com.lmax.disruptor.EventFactory;
import com.lmax.disruptor.SleepingWaitStrategy;
import com.lmax.disruptor.WaitStrategy;
import com.lmax.disruptor.dsl.ProducerType;

/**
 * @author Egor Litvinenko
 */
public class StringToParseDoubleDisruptor extends AbstractDisruptorFactory<StringToParseDoubleEvent> {

    @Override
    protected EventFactory<StringToParseDoubleEvent> createEventFactory() {
        return new StringToParseDoubleEventFactory();
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
        return ByteStreamParsingConstants.STRING_TO_PARSE_DOUBLE_BUFFER;
    }
}
