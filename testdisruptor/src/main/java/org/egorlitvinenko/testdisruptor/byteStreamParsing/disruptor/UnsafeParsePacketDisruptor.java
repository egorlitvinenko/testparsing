package org.egorlitvinenko.testdisruptor.byteStreamParsing.disruptor;

import com.lmax.disruptor.*;
import com.lmax.disruptor.dsl.Disruptor;
import com.lmax.disruptor.dsl.ProducerType;
import org.apache.commons.lang3.NotImplementedException;
import org.egorlitvinenko.testdisruptor.byteStreamParsing.event.UnsafeParsePacketEvent;
import org.egorlitvinenko.testdisruptor.byteStreamParsing.handler.unsafeParsePacket.ClickhouseUnsafeParsePacketHandler;
import org.egorlitvinenko.testdisruptor.byteStreamParsing.model.RowModel;
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
public class UnsafeParsePacketDisruptor extends AbstractDisruptorFactory<UnsafeParsePacketEvent> {

    private final RowModel rowModel;

    public UnsafeParsePacketDisruptor(ColumnType[] types) {
        rowModel = new RowModel(types);
    }

    public Disruptor<UnsafeParsePacketEvent> createWriteToClickhouse(ThreadFactory threadFactory,
                                                               ColumnType[] types,
                                                                     ClickhouseUnsafeParsePacketHandler clickhouseHandler) {
        // The factory for the event
        EventFactory<UnsafeParsePacketEvent> factory = createEventFactory();

        // Specify the size of the ring buffer, must be power of 2.
        int bufferSize = getRingBufferSize();

        // Construct the Disruptor
        Disruptor<UnsafeParsePacketEvent> disruptor =
                new Disruptor<>(factory, bufferSize, threadFactory,
                        getProducerType(), getWaitStrategy());

        // Connect the handler
        final EventHandler<UnsafeParsePacketEvent>[] parseHandlers = getParseHandlers(types);
        if (parseHandlers.length > 0) {
            disruptor
                    .handleEventsWith(parseHandlers)
                    .then(clickhouseHandler);
        } else {
            throw new IllegalArgumentException("Nothing to parse");
        }

        // Start the Disruptor, starts all threads running
        RingBuffer<UnsafeParsePacketEvent> ringBuffer = disruptor.start();

        return disruptor;
    }


    @Override
    protected EventFactory<UnsafeParsePacketEvent> createEventFactory() {
        return new EventFactory<UnsafeParsePacketEvent>() {
            @Override
            public UnsafeParsePacketEvent newInstance() {
                return new UnsafeParsePacketEvent(
                        UnsafeParsePacketDisruptor.this.rowModel);
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


    public static EventHandler<UnsafeParsePacketEvent>[] getParseHandlers(ColumnType[] types) {
        Set<ColumnType> typeSet = Arrays.stream(types).collect(Collectors.toSet());
        List<EventHandler<UnsafeParsePacketEvent>> handlers = new ArrayList<>();
        for (ColumnType type : typeSet) {
            switch (type) {
                case INT_32:
                    handlers.add(
                            new org.egorlitvinenko.testdisruptor.byteStreamParsing.handler.unsafeParsePacket.StringToInt32ParsePacketHandler(
                            ParsePacketCsvDisruptor.typeColumns(types, ColumnType.INT_32)
                    ));
                    break;
                case DOUBLE:
                    handlers.add(new org.egorlitvinenko.testdisruptor.byteStreamParsing.handler.unsafeParsePacket.StringToDoubleParsePacketHandler(
                            ParsePacketCsvDisruptor.typeColumns(types, ColumnType.DOUBLE)
                    ));
                    break;
                case LOCAL_DATE:
                    throw new NotImplementedException("1");
                case SQL_DATE:
                    handlers.add(new org.egorlitvinenko.testdisruptor.byteStreamParsing.handler.unsafeParsePacket.StringToSqlDateParsePacketHandler(
                            ParsePacketCsvDisruptor.typeColumns(types, ColumnType.SQL_DATE)
                    ));
                    break;
            }
        }
        EventHandler<UnsafeParsePacketEvent>[] result = new EventHandler[handlers.size()];
        for (int i = 0; i < handlers.size(); ++i) result[i] = handlers.get(i);
        return result;
    }

}
