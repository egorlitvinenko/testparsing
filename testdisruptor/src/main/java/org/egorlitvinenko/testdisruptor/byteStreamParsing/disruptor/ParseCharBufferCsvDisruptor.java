package org.egorlitvinenko.testdisruptor.byteStreamParsing.disruptor;

import com.lmax.disruptor.EventFactory;
import com.lmax.disruptor.EventHandler;
import com.lmax.disruptor.SleepingWaitStrategy;
import com.lmax.disruptor.WaitStrategy;
import com.lmax.disruptor.dsl.Disruptor;
import com.lmax.disruptor.dsl.ProducerType;
import org.egorlitvinenko.testdisruptor.byteStreamParsing.ByteStreamParsingConstants;
import org.egorlitvinenko.testdisruptor.byteStreamParsing.event.ParseCharBufferTableRowEvent;
import org.egorlitvinenko.testdisruptor.byteStreamParsing.handler.batchAndCompareWithType.ClickhouseParseCharBufferTableRowHandler;
import org.egorlitvinenko.testdisruptor.byteStreamParsing.handler.charBufferAndcompareWithType.*;
import org.egorlitvinenko.testdisruptor.byteStreamParsing.parsing.spi.PositionedIsoSqlDateParser;
import org.egorlitvinenko.testdisruptor.byteStreamParsing.parsing.spi.SimpleDoubleValueOf;
import org.egorlitvinenko.testdisruptor.byteStreamParsing.parsing.spi.SimpleIntegerValueOf;
import org.egorlitvinenko.testdisruptor.byteStreamParsing.parsing.spi.SimpleIsoLocalDateParser;
import org.egorlitvinenko.testdisruptor.byteStreamParsing.util.ColumnType;
import org.egorlitvinenko.testdisruptor.smallstream.disruptor.AbstractDisruptorFactory;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ThreadFactory;
import java.util.stream.Collectors;

/**
 * @author Egor Litvinenko
 */
public class ParseCharBufferCsvDisruptor extends AbstractDisruptorFactory<ParseCharBufferTableRowEvent> {

    int columnSize;

    public Disruptor<ParseCharBufferTableRowEvent> createWriteToClickhouse(ThreadFactory threadFactory,
                                                                      ColumnType[] types,
                                                                      ClickhouseParseCharBufferTableRowHandler clickhouseHandler) {
        columnSize = types.length;
        // The factory for the event
        EventFactory<ParseCharBufferTableRowEvent> factory = createEventFactory();

        // Specify the size of the ring buffer, must be power of 2.
        int bufferSize = getRingBufferSize();

        // Construct the Disruptor
        Disruptor<ParseCharBufferTableRowEvent> disruptor =
                new Disruptor<>(factory, bufferSize, threadFactory,
                        getProducerType(), getWaitStrategy());

        // Connect the handler
        final EventHandler<ParseCharBufferTableRowEvent>[] parseHandlers = getParseHandlers(types);
        if (parseHandlers.length > 0) {
            disruptor
                    .handleEventsWith(parseHandlers)
                    .then(clickhouseHandler);
        } else {
            throw new IllegalArgumentException("Nothing to parse");
        }

        // Start the Disruptor, starts all threads running
        disruptor.start();

        return disruptor;
    }

    @Override
    protected EventFactory<ParseCharBufferTableRowEvent> createEventFactory() {
        return new EventFactory<ParseCharBufferTableRowEvent>() {
            @Override
            public ParseCharBufferTableRowEvent newInstance() {
                return new ParseCharBufferTableRowEvent(columnSize);
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


    public static EventHandler<ParseCharBufferTableRowEvent>[] getParseHandlers(ColumnType[] types) {
        Set<ColumnType> typeSet = Arrays.stream(types).collect(Collectors.toSet());
        List<EventHandler<ParseCharBufferTableRowEvent>> handlers = new ArrayList<>();
        for (ColumnType type : typeSet) {
            switch (type) {
                case INT_32:
                    handlers.add(new StringToInt32ParseCharBufferHandler(
                            new SimpleIntegerValueOf(),
                            ParsePacketCsvDisruptor.typeColumns(types, ColumnType.INT_32)
                    ));
                    break;
                case DOUBLE:
                    handlers.add(new StringToDoubleParseCharBufferHandler(
                            new SimpleDoubleValueOf(),
                            ParsePacketCsvDisruptor.typeColumns(types, ColumnType.DOUBLE)
                    ));
                    break;
                case LOCAL_DATE:
                    handlers.add(new StringToLocalDateParseCharBufferHandler(
                            new SimpleIsoLocalDateParser(),
                            ParsePacketCsvDisruptor.typeColumns(types, ColumnType.LOCAL_DATE)
                    ));
                    break;
                case SQL_DATE:
                    handlers.add(new StringToSqlDateParseCharBufferHandler(
                            new PositionedIsoSqlDateParser(),
                            ParsePacketCsvDisruptor.typeColumns(types, ColumnType.SQL_DATE)
                    ));
                    break;
            }
        }
        EventHandler<ParseCharBufferTableRowEvent>[] result = new EventHandler[handlers.size()];
        for (int i = 0; i < handlers.size(); ++i) result[i] = handlers.get(i);
        return result;
    }
}
