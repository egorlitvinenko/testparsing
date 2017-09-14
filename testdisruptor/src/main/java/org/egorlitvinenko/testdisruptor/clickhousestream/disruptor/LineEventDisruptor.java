package org.egorlitvinenko.testdisruptor.clickhousestream.disruptor;

import com.lmax.disruptor.EventFactory;
import com.lmax.disruptor.EventHandler;
import com.lmax.disruptor.SleepingWaitStrategy;
import com.lmax.disruptor.WaitStrategy;
import com.lmax.disruptor.dsl.Disruptor;
import com.lmax.disruptor.dsl.ProducerType;
import org.apache.commons.lang3.NotImplementedException;
import org.egorlitvinenko.testdisruptor.byteStreamParsing.disruptor.ParsePacketCsvDisruptor;
import org.egorlitvinenko.testdisruptor.byteStreamParsing.util.ColumnType;
import org.egorlitvinenko.testdisruptor.clickhousestream.ChLineEventHandler;
import org.egorlitvinenko.testdisruptor.clickhousestream.event.LineEvent;
import org.egorlitvinenko.testdisruptor.clickhousestream.handler.DoubleValidation;
import org.egorlitvinenko.testdisruptor.clickhousestream.handler.IntegerValidation;
import org.egorlitvinenko.testdisruptor.clickhousestream.handler.SqlDateValidation;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ThreadFactory;
import java.util.stream.Collectors;

/**
 * @author Egor Litvinenko
 */
public class LineEventDisruptor {

    public Disruptor<LineEvent> create(ThreadFactory threadFactory,
                                       ColumnType[] types,
                                       ChLineEventHandler clickhouseHandler) {
        final int threads = typesCount(types);

        EventFactory<LineEvent> factory = () -> new LineEvent(threads);

        // Specify the size of the ring buffer, must be power of 2.
        int bufferSize = getRingBufferSize();

        // Construct the Disruptor
        Disruptor<LineEvent> disruptor =
                new Disruptor<>(factory, bufferSize, threadFactory,
                        getProducerType(), getWaitStrategy());

        // Connect the handler
        final EventHandler<LineEvent>[] parseHandlers = getParseHandlers(types);
        if (parseHandlers.length > 0) {
            disruptor
                    .handleEventsWith(parseHandlers)
                    .then(clickhouseHandler);
        } else {
            throw new IllegalArgumentException("Nothing to parse");
        }

        disruptor.start();

        return disruptor;
    }

    protected int getRingBufferSize() {
        return 1024 * 32;
    }

    protected WaitStrategy getWaitStrategy() {
        return new SleepingWaitStrategy();
    }

    protected ProducerType getProducerType() {
        return ProducerType.SINGLE;
    }

    public static int typesCount(ColumnType[] types) {
        return Arrays.stream(types).collect(Collectors.toSet()).size();
    }

    public static EventHandler<LineEvent>[] getParseHandlers(ColumnType[] types) {
        Set<ColumnType> typeSet = Arrays.stream(types).collect(Collectors.toSet());
        List<EventHandler<LineEvent>> handlers = new ArrayList<>();
        int id = 0;
        for (ColumnType type : typeSet) {
            switch (type) {
                case INT_32:
                    handlers.add(new IntegerValidation(id++,
                            ParsePacketCsvDisruptor.typeColumns(types, ColumnType.INT_32)));
                    break;
                case DOUBLE:
                    handlers.add(new DoubleValidation(id++,
                            ParsePacketCsvDisruptor.typeColumns(types, ColumnType.DOUBLE)));
                    break;
                case LOCAL_DATE:
                    throw new NotImplementedException("LOCAL_DATE");
                case SQL_DATE:
                    handlers.add(new SqlDateValidation(id++,
                            ParsePacketCsvDisruptor.typeColumns(types, ColumnType.SQL_DATE)));
                    break;
            }
        }
        EventHandler<LineEvent>[] result = new EventHandler[handlers.size()];
        for (int i = 0; i < handlers.size(); ++i) result[i] = handlers.get(i);
        return result;
    }


}
