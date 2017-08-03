package org.egorlitvinenko.testdisruptor.byteStreamParsing.disruptor;

import org.egorlitvinenko.testdisruptor.byteStreamParsing.ByteStreamParsingConstants;
import org.egorlitvinenko.testdisruptor.byteStreamParsing.event.ParsedTableRowEvent;
import org.egorlitvinenko.testdisruptor.byteStreamParsing.factory.ParsedTableRowEventFactory;
import org.egorlitvinenko.testdisruptor.smallstream.disruptor.AbstractDisruptorFactory;
import com.lmax.disruptor.BlockingWaitStrategy;
import com.lmax.disruptor.EventFactory;
import com.lmax.disruptor.WaitStrategy;
import com.lmax.disruptor.dsl.ProducerType;

/**
 * @author Egor Litvinenko
 */
public class ParsedTableRowDisruptor extends AbstractDisruptorFactory<ParsedTableRowEvent> {

    @Override
    protected EventFactory<ParsedTableRowEvent> createEventFactory() {
        return new ParsedTableRowEventFactory();
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
        return ByteStreamParsingConstants.PARSED_TABLE_ROW_BUFFER;
    }

}
