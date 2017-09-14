package org.egorlitvinenko.testdisruptor.byteStreamParsing.disruptor;

import com.lmax.disruptor.*;
import com.lmax.disruptor.dsl.Disruptor;
import com.lmax.disruptor.dsl.ProducerType;
import org.egorlitvinenko.testdisruptor.byteStreamParsing.event.ParsePacketEvent;
import org.egorlitvinenko.testdisruptor.byteStreamParsing.handler.parsePacket.ClickhouseParsePacketHandler;
import org.egorlitvinenko.testdisruptor.byteStreamParsing.handler.parsePacket.ReadLineHandler;
import org.egorlitvinenko.testdisruptor.byteStreamParsing.model.RowModel;
import org.egorlitvinenko.testdisruptor.byteStreamParsing.model.TableRowIndexModel;
import org.egorlitvinenko.testdisruptor.byteStreamParsing.model.TableRowTypeModel;
import org.egorlitvinenko.testdisruptor.byteStreamParsing.parsing.spi.PositionedIsoLocalDateParser;
import org.egorlitvinenko.testdisruptor.byteStreamParsing.parsing.spi.PositionedIsoSqlDateParser;
import org.egorlitvinenko.testdisruptor.byteStreamParsing.parsing.spi.SimpleDoubleValueOf;
import org.egorlitvinenko.testdisruptor.byteStreamParsing.parsing.spi.SimpleIntegerValueOf;
import org.egorlitvinenko.testdisruptor.byteStreamParsing.util.ColumnType;
import org.egorlitvinenko.testdisruptor.smallstream.disruptor.AbstractDisruptorFactory;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ThreadFactory;
import java.util.function.Consumer;
import java.util.stream.Collectors;

/**
 * @author Egor Litvinenko
 */
public class ParsePacketDisruptor extends AbstractDisruptorFactory<ParsePacketEvent> {

    private final TableRowTypeModel typeModel;
    private final TableRowIndexModel indexModel;

    public ParsePacketDisruptor(ColumnType[] types) {
        typeModel = new TableRowTypeModel(types);
        indexModel = new TableRowIndexModel(types);
    }

    public Disruptor<ParsePacketEvent> createReadAndWriteToClickhouse(ThreadFactory threadFactory,
                                                                      String file,
                                                                      char delimiter,
                                                                      char quote,
                                                                      Consumer<Boolean> endConsumer,
                                                                      ColumnType[] types,
                                                                      ClickhouseParsePacketHandler clickhouseHandler) {
        // The factory for the event
        EventFactory<ParsePacketEvent> factory = createEventFactory();

        // Specify the size of the ring buffer, must be power of 2.
        int bufferSize = getRingBufferSize();

        // Construct the Disruptor
        Disruptor<ParsePacketEvent> disruptor =
                new Disruptor<>(factory, bufferSize, threadFactory,
                        getProducerType(), getWaitStrategy());

        // Connect the handler
        final EventHandler<ParsePacketEvent>[] parseHandlers = getParseHandlers(types);
        if (parseHandlers.length > 0) {
            disruptor
                    .handleEventsWith(new ReadLineHandler(
                            file,
                            delimiter,
                            quote,
                            types,
                            endConsumer))
                    .then(parseHandlers)
                    .then(clickhouseHandler);
        } else {
            throw new IllegalArgumentException("Nothing to parse");
        }

        // Start the Disruptor, starts all threads running
        RingBuffer<ParsePacketEvent> ringBuffer = disruptor.start();

        return disruptor;
    }

    public Disruptor<ParsePacketEvent> createWriteToClickhouse(ThreadFactory threadFactory,
                                                               ColumnType[] types,
                                                               ClickhouseParsePacketHandler clickhouseHandler) {
        // The factory for the event
        EventFactory<ParsePacketEvent> factory = createEventFactory();

        // Specify the size of the ring buffer, must be power of 2.
        int bufferSize = getRingBufferSize();

        // Construct the Disruptor
        Disruptor<ParsePacketEvent> disruptor =
                new Disruptor<>(factory, bufferSize, threadFactory,
                        getProducerType(), getWaitStrategy());

        // Connect the handler
        final EventHandler<ParsePacketEvent>[] parseHandlers = getParseHandlers(types);
        if (parseHandlers.length > 0) {
            disruptor
                    .handleEventsWith(parseHandlers)
                    .then(clickhouseHandler);
        } else {
            throw new IllegalArgumentException("Nothing to parse");
        }

        // Start the Disruptor, starts all threads running
        RingBuffer<ParsePacketEvent> ringBuffer = disruptor.start();

        return disruptor;
    }


    @Override
    protected EventFactory<ParsePacketEvent> createEventFactory() {
        return new EventFactory<ParsePacketEvent>() {
            @Override
            public ParsePacketEvent newInstance() {
                return new ParsePacketEvent(
                        ParsePacketDisruptor.this.indexModel,
                        ParsePacketDisruptor.this.typeModel);
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
        return 1024 * 32;
    }


    public static EventHandler<ParsePacketEvent>[] getParseHandlers(ColumnType[] types) {
        Set<ColumnType> typeSet = Arrays.stream(types).collect(Collectors.toSet());
        List<EventHandler<ParsePacketEvent>> handlers = new ArrayList<>();
        for (ColumnType type : typeSet) {
            switch (type) {
                case INT_32:
                    handlers.add(new org.egorlitvinenko.testdisruptor.byteStreamParsing.handler.parsePacket.StringToInt32ParsePacketHandler(
                            new SimpleIntegerValueOf(),
                            ParsePacketCsvDisruptor.typeColumns(types, ColumnType.INT_32)
                    ));
                    break;
                case DOUBLE:
                    handlers.add(new org.egorlitvinenko.testdisruptor.byteStreamParsing.handler.parsePacket.StringToDoubleParsePacketHandler(
                            new SimpleDoubleValueOf(),
                            ParsePacketCsvDisruptor.typeColumns(types, ColumnType.DOUBLE)
                    ));
                    break;
                case LOCAL_DATE:
                    handlers.add(new org.egorlitvinenko.testdisruptor.byteStreamParsing.handler.parsePacket.StringToLocalDateParsePacketHandler(
                            new PositionedIsoLocalDateParser(),
                            ParsePacketCsvDisruptor.typeColumns(types, ColumnType.LOCAL_DATE)
                    ));
                    break;
                case SQL_DATE:
                    handlers.add(new org.egorlitvinenko.testdisruptor.byteStreamParsing.handler.parsePacket.StringToSqlDateParsePacketHandler(
                            new PositionedIsoSqlDateParser(),
                            ParsePacketCsvDisruptor.typeColumns(types, ColumnType.SQL_DATE)
                    ));
                    break;
            }
        }
        EventHandler<ParsePacketEvent>[] result = new EventHandler[handlers.size()];
        for (int i = 0; i < handlers.size(); ++i) result[i] = handlers.get(i);
        return result;
    }

}
