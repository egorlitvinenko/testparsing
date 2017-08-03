package org.egorlitvinenko.testdisruptor.byteStreamParsing.disruptor;

import org.egorlitvinenko.testdisruptor.byteStreamParsing.ByteStreamParsingConstants;
import org.egorlitvinenko.testdisruptor.byteStreamParsing.event.ParseTableRowEvent;
import org.egorlitvinenko.testdisruptor.byteStreamParsing.handler.splittedByColumn.*;
import org.egorlitvinenko.testdisruptor.byteStreamParsing.parsing.spi.SimpleDoubleValueOf;
import org.egorlitvinenko.testdisruptor.byteStreamParsing.parsing.spi.SimpleIntegerValueOf;
import org.egorlitvinenko.testdisruptor.byteStreamParsing.parsing.spi.SimpleIsoLocalDateParser;
import org.egorlitvinenko.testdisruptor.byteStreamParsing.util.ColumnType;
import org.egorlitvinenko.testdisruptor.smallstream.disruptor.AbstractDisruptorFactory;
import com.lmax.disruptor.EventFactory;
import com.lmax.disruptor.RingBuffer;
import com.lmax.disruptor.SleepingWaitStrategy;
import com.lmax.disruptor.WaitStrategy;
import com.lmax.disruptor.dsl.Disruptor;
import com.lmax.disruptor.dsl.ProducerType;

import java.util.concurrent.ThreadFactory;

/**
 * @author Egor Litvinenko
 */
public class ParseCsvDisruptor extends AbstractDisruptorFactory<ParseTableRowEvent> {

    private final CountParsedTableRowHandler2 countParsedTableRowHandler2 = new CountParsedTableRowHandler2();

    public Disruptor<ParseTableRowEvent> createWriteToClickhouse(ThreadFactory threadFactory,
                                                                 ColumnType[] types,
                                                                 ClickhouseParsedTableRowHandler2 handler2) {
        // The factory for the event
        EventFactory<ParseTableRowEvent> factory = createEventFactory();

        // Specify the size of the ring buffer, must be power of 2.
        int bufferSize = getRingBufferSize();

        // Construct the Disruptor
        Disruptor<ParseTableRowEvent> disruptor =
                new Disruptor<>(factory, bufferSize, threadFactory,
                        getProducerType(), getWaitStrategy());

        // Connect the handler
        disruptor
                .handleEventsWith(
                        new StringToDoubleParseHandler2(
                                new SimpleDoubleValueOf(),
                                markedColumns(types, ColumnType.DOUBLE)),
                        new StringToInt32ParseHandler2(
                                new SimpleIntegerValueOf(),
                                markedColumns(types, ColumnType.INT_32)),
                        new StringToLocalDateParseHandler2(
                                new SimpleIsoLocalDateParser(),
                                markedColumns(types, ColumnType.LOCAL_DATE)))
                .then(handler2);

        // Start the Disruptor, starts all threads running
        RingBuffer<ParseTableRowEvent> ringBuffer = disruptor.start();

        return disruptor;
    }

    public Disruptor<ParseTableRowEvent> createCounter(ThreadFactory threadFactory,
                                                       ColumnType[] types) {
        // The factory for the event
        EventFactory<ParseTableRowEvent> factory = createEventFactory();

        // Specify the size of the ring buffer, must be power of 2.
        int bufferSize = getRingBufferSize();

        // Construct the Disruptor
        Disruptor<ParseTableRowEvent> disruptor =
                new Disruptor<>(factory, bufferSize, threadFactory,
                        getProducerType(), getWaitStrategy());

        // Connect the handler
        disruptor
                .handleEventsWith(
                        new StringToDoubleParseHandler2(
                                new SimpleDoubleValueOf(),
                                markedColumns(types, ColumnType.DOUBLE)),
                        new StringToInt32ParseHandler2(
                                new SimpleIntegerValueOf(),
                                markedColumns(types, ColumnType.INT_32)),
                        new StringToLocalDateParseHandler2(
                                new SimpleIsoLocalDateParser(),
                                markedColumns(types, ColumnType.LOCAL_DATE)))
                .then(countParsedTableRowHandler2);

        // Start the Disruptor, starts all threads running
        RingBuffer<ParseTableRowEvent> ringBuffer = disruptor.start();

        return disruptor;
    }

    public int getCounter() {
        return countParsedTableRowHandler2.getCounter();
    }

    public int getCalls() {
        return countParsedTableRowHandler2.getCalls();
    }

    private static boolean[] markedColumns(ColumnType[] types, ColumnType columnType) {
        boolean[] result = new boolean[types.length];
        for (int i = 0; i < result.length; ++i) {
            result[i] = types[i].equals(columnType);
        }
        return result;
    }

    @Override
    protected EventFactory<ParseTableRowEvent> createEventFactory() {
        return new EventFactory<ParseTableRowEvent>() {
            @Override
            public ParseTableRowEvent newInstance() {
                return new ParseTableRowEvent();
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
        return ByteStreamParsingConstants.PARSED_TABLE_ROW_BUFFER;
    }
}
