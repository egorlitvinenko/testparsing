package org.egorlitvinenko.testdisruptor.smallstream.handler;

import org.egorlitvinenko.testdisruptor.smallstream.event.NewBatchEvent;
import org.egorlitvinenko.testdisruptor.smallstream.event.NewParsedBatchEvent;
import org.egorlitvinenko.testdisruptor.smallstream.util.ArrayUtils;
import org.egorlitvinenko.testdisruptor.smallstream.util.Types;
import com.lmax.disruptor.EventHandler;
import com.lmax.disruptor.EventTranslatorThreeArg;
import com.lmax.disruptor.RingBuffer;

import java.time.LocalDate;
import java.time.format.DateTimeFormatter;

/**
 * @author Egor Litvinenko
 */
public class BatchParser implements EventHandler<NewBatchEvent> {

    private static DateTimeFormatter DATE_FORMATTER = DateTimeFormatter.ISO_LOCAL_DATE;

    private final byte[] types;
    private final RingBuffer<NewParsedBatchEvent> newParsedBatchEventRingBuffer;

    public BatchParser(byte[] types, RingBuffer<NewParsedBatchEvent> newParsedBatchEventRingBuffer) {
        this.types = types;
        this.newParsedBatchEventRingBuffer = newParsedBatchEventRingBuffer;
    }

    @Override
    public void onEvent(NewBatchEvent event, long sequence, boolean endOfBatch) throws Exception {
        String[] line;
        Object[][] parsedBatch = ArrayUtils.allocateObject2DArray(event.getSize(), types.length);
        for (int i = 0; i < event.getSize(); ++i) {
            line = event.getBatch()[i];
            for (int j = 0; j < line.length; ++j) {
                parsedBatch[i][j] = parseValue(types[j], event.getBatch()[i][j]);
            }
        }
        this.newParsedBatchEventRingBuffer.publishEvent(TRANSLATOR,
                parsedBatch,
                event.getSize(), this.types);
    }

    private static Object parseValue(byte type, String value) {
        switch (type) {
            case Types.DOUBLE:
                return Double.valueOf(value);
            case Types.INT_32:
                return Integer.valueOf(value);
            case Types.LOCAL_DATE:
                return LocalDate.from(DATE_FORMATTER.parse(value));
            case Types.STRING:
            default:
                return value;
        }
    }

    private static EventTranslatorThreeArg<NewParsedBatchEvent, Object[][], Integer, byte[]> TRANSLATOR = new EventTranslatorThreeArg<NewParsedBatchEvent, Object[][], Integer, byte[]>() {
        @Override
        public void translateTo(NewParsedBatchEvent event, long sequence, Object[][] arg0, Integer arg1, byte[] arg2) {
            event.setSize(arg1);
            event.setBatch(arg0);
            event.setTypes(arg2);
        }
    };

}
