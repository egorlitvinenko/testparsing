package org.egorlitvinenko.testdisruptor.byteStreamParsing.disruptor;

import org.egorlitvinenko.testdisruptor.byteStreamParsing.ByteStreamParsingConstants;
import org.egorlitvinenko.testdisruptor.byteStreamParsing.event.ParsePacketTableRowEvent;
import org.egorlitvinenko.testdisruptor.byteStreamParsing.handler.compareWithType.*;
import org.egorlitvinenko.testdisruptor.byteStreamParsing.handler.splittedByColumn.CountParsedTableRowHandler2;
import org.egorlitvinenko.testdisruptor.byteStreamParsing.parsing.spi.PositionedIsoLocalDateParser;
import org.egorlitvinenko.testdisruptor.byteStreamParsing.parsing.spi.PositionedIsoSqlDateParser;
import org.egorlitvinenko.testdisruptor.byteStreamParsing.parsing.spi.SimpleDoubleValueOf;
import org.egorlitvinenko.testdisruptor.byteStreamParsing.parsing.spi.SimpleIntegerValueOf;
import org.egorlitvinenko.testdisruptor.byteStreamParsing.util.ColumnType;
import org.egorlitvinenko.testdisruptor.smallstream.disruptor.AbstractDisruptorFactory;
import com.lmax.disruptor.*;
import com.lmax.disruptor.dsl.Disruptor;
import com.lmax.disruptor.dsl.ProducerType;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ThreadFactory;
import java.util.stream.Collectors;

/**
 * @author Egor Litvinenko
 */
public class ParsePacketCsvDisruptor extends AbstractDisruptorFactory<ParsePacketTableRowEvent> {

    private final CountParsedTableRowHandler2 countParsedTableRowHandler2 = new CountParsedTableRowHandler2();

    public Disruptor<ParsePacketTableRowEvent> createWriteToClickhouse(ThreadFactory threadFactory,
                                                                       ColumnType[] types,
                                                                       ClickhouseParsePacketTableRowHandler clickhouseHandler) {
        // The factory for the event
        EventFactory<ParsePacketTableRowEvent> factory = createEventFactory();

        // Specify the size of the ring buffer, must be power of 2.
        int bufferSize = getRingBufferSize();

        // Construct the Disruptor
        Disruptor<ParsePacketTableRowEvent> disruptor =
                new Disruptor<>(factory, bufferSize, threadFactory,
                        getProducerType(), getWaitStrategy());

        // Connect the handler
        final EventHandler<ParsePacketTableRowEvent>[] parseHandlers = getParseHandlers(types);
        if (parseHandlers.length > 0) {
            disruptor
                    .handleEventsWith(parseHandlers)
                    .then(clickhouseHandler);
        } else {
            throw new IllegalArgumentException("Nothing to parse");
        }

        // Start the Disruptor, starts all threads running
        RingBuffer<ParsePacketTableRowEvent> ringBuffer = disruptor.start();

        return disruptor;
    }

    public int getCounter() {
        return countParsedTableRowHandler2.getCounter();
    }

    public int getCalls() {
        return countParsedTableRowHandler2.getCalls();
    }

    public static int[] typeColumns(ColumnType[] types, ColumnType columnType) {
        List<Integer> result = new ArrayList<>();
        for (int i = 0; i < types.length; ++i) {
            if (types[i] == columnType) {
                result.add(i);
            }
        }
        int[] result2 = new int[result.size()];
        for (int i = 0; i < result.size(); ++i) {
            result2[i] = result.get(i);
        }
        return result2;
    }

    @Override
    protected EventFactory<ParsePacketTableRowEvent> createEventFactory() {
        return new EventFactory<ParsePacketTableRowEvent>() {
            @Override
            public ParsePacketTableRowEvent newInstance() {
                return new ParsePacketTableRowEvent();
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


    public static EventHandler<ParsePacketTableRowEvent>[] getParseHandlers(ColumnType[] types) {
        Set<ColumnType> typeSet = Arrays.stream(types).collect(Collectors.toSet());
        List<EventHandler<ParsePacketTableRowEvent>> handlers = new ArrayList<>();
        for (ColumnType type : typeSet) {
            switch (type) {
                case INT_32:
                    handlers.add(new StringToInt32ParsePacketHandler(
                            new SimpleIntegerValueOf(),
                            typeColumns(types, ColumnType.INT_32)
                    ));
                    break;
                case DOUBLE:
                    handlers.add(new StringToDoubleParsePacketHandler(
                            new SimpleDoubleValueOf(),
                            typeColumns(types, ColumnType.DOUBLE)
                    ));
                    break;
                case LOCAL_DATE:
                    handlers.add(new StringToLocalDateParsePacketHandler(
                            new PositionedIsoLocalDateParser(),
                            typeColumns(types, ColumnType.LOCAL_DATE)
                    ));
                    break;
                case SQL_DATE:
                    handlers.add(new StringToSqlDateParsePacketHandler(
                            new PositionedIsoSqlDateParser(),
                            typeColumns(types, ColumnType.SQL_DATE)
                    ));
                    break;
            }
        }
        EventHandler<ParsePacketTableRowEvent>[] result = new EventHandler[handlers.size()];
        for (int i = 0; i < handlers.size(); ++i) result[i] = handlers.get(i);
        return result;
    }
}
