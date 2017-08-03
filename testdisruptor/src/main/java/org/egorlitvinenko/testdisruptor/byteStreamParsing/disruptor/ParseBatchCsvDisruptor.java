package org.egorlitvinenko.testdisruptor.byteStreamParsing.disruptor;

import com.lmax.disruptor.EventFactory;
import com.lmax.disruptor.EventHandler;
import com.lmax.disruptor.SleepingWaitStrategy;
import com.lmax.disruptor.WaitStrategy;
import com.lmax.disruptor.dsl.Disruptor;
import com.lmax.disruptor.dsl.ProducerType;
import org.egorlitvinenko.testdisruptor.byteStreamParsing.ByteStreamParsingConstants;
import org.egorlitvinenko.testdisruptor.byteStreamParsing.event.ParseBatchTableRowEvent;
import org.egorlitvinenko.testdisruptor.byteStreamParsing.handler.batchAndCompareWithType.*;
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
public class ParseBatchCsvDisruptor extends AbstractDisruptorFactory<ParseBatchTableRowEvent> {

    public Disruptor<ParseBatchTableRowEvent> createWriteToClickhouse(ThreadFactory threadFactory,
                                                                       ColumnType[] types,
                                                                       ClickhouseParseBatchTableRowHandler clickhouseHandler) {
        // The factory for the event
        EventFactory<ParseBatchTableRowEvent> factory = createEventFactory();

        // Specify the size of the ring buffer, must be power of 2.
        int bufferSize = getRingBufferSize();

        // Construct the Disruptor
        Disruptor<ParseBatchTableRowEvent> disruptor =
                new Disruptor<>(factory, bufferSize, threadFactory,
                        getProducerType(), getWaitStrategy());

        // Connect the handler
        final EventHandler<ParseBatchTableRowEvent>[] parseHandlers = getParseHandlers(types);
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
    protected EventFactory<ParseBatchTableRowEvent> createEventFactory() {
        return new EventFactory<ParseBatchTableRowEvent>() {
            @Override
            public ParseBatchTableRowEvent newInstance() {
                return new ParseBatchTableRowEvent();
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


    public static EventHandler<ParseBatchTableRowEvent>[] getParseHandlers(ColumnType[] types) {
        Set<ColumnType> typeSet = Arrays.stream(types).collect(Collectors.toSet());
        List<EventHandler<ParseBatchTableRowEvent>> handlers = new ArrayList<>();
        for (ColumnType type : typeSet) {
            switch (type) {
                case INT_32:
                    handlers.add(new StringToInt32ParseBatchHandler(
                            new SimpleIntegerValueOf(),
                            ParsePacketCsvDisruptor.typeColumns(types, ColumnType.INT_32)
                    ));
                    break;
                case DOUBLE:
                    handlers.add(new StringToDoubleParseBatchHandler(
                            new SimpleDoubleValueOf(),
                            ParsePacketCsvDisruptor.typeColumns(types, ColumnType.DOUBLE)
                    ));
                    break;
                case LOCAL_DATE:
                    handlers.add(new StringToLocalDateParseBatchHandler(
                            new SimpleIsoLocalDateParser(),
                            ParsePacketCsvDisruptor.typeColumns(types, ColumnType.LOCAL_DATE)
                    ));
                    break;
                case SQL_DATE:
                    handlers.add(new StringToSqlDateParseBatchHandler(
                            new PositionedIsoSqlDateParser(),
                            ParsePacketCsvDisruptor.typeColumns(types, ColumnType.SQL_DATE)
                    ));
                    break;
            }
        }
        EventHandler<ParseBatchTableRowEvent>[] result = new EventHandler[handlers.size()];
        for (int i = 0; i < handlers.size(); ++i) result[i] = handlers.get(i);
        return result;
    }
}
