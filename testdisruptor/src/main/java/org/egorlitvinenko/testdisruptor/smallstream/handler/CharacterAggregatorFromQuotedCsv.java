package org.egorlitvinenko.testdisruptor.smallstream.handler;

import org.egorlitvinenko.testdisruptor.smallstream.event.NewByteEvent;
import org.egorlitvinenko.testdisruptor.smallstream.event.NewStringEvent;
import com.lmax.disruptor.EventHandler;
import com.lmax.disruptor.EventTranslatorOneArg;
import com.lmax.disruptor.RingBuffer;

import java.nio.ByteBuffer;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;

/**
 * @author Egor Litvinenko
 */
public class CharacterAggregatorFromQuotedCsv implements EventHandler<NewByteEvent> {

    private final ByteBuffer aggregate;
    private int aggregateCounter;

    private final byte delimiter, lineEnd, quote;
    private final Charset charset = StandardCharsets.UTF_8;
    private final RingBuffer<NewStringEvent> stringBuffer;

    public CharacterAggregatorFromQuotedCsv(byte lineEnd, byte delimiter, byte quote,
                                            RingBuffer<NewStringEvent> stringBuffer) {
        this.aggregate = ByteBuffer.allocate(100);
        this.delimiter = delimiter;
        this.lineEnd = lineEnd;
        this.quote = quote;
        this.stringBuffer = stringBuffer;
    }

    @Override
    public void onEvent(NewByteEvent newByteEvent, long sequence, boolean endOfBatch) throws Exception {
        if (newByteEvent.getCharacter() == delimiter) {
            this.stringBuffer.publishEvent(MIDDLE_STRING_TRANSLATOR, getString(aggregate, aggregateCounter, charset));
            this.aggregateCounter = 0;
        } else if (newByteEvent.getCharacter() == lineEnd) {
            this.stringBuffer.publishEvent(LAST_STRING_TRANSLATOR,   getString(aggregate, aggregateCounter, charset));
            this.aggregateCounter = 0;
        } else if (newByteEvent.getCharacter() != quote) {
            aggregate.put(aggregateCounter++, newByteEvent.getCharacter());
        }
    }

    private static String getString(ByteBuffer byteBuffer, int length, Charset charset) {
        byte[] temp = new byte[length];
        System.arraycopy(byteBuffer.array(), 0, temp, 0, length);
        return new String(temp, charset);
    }

    private static final EventTranslatorOneArg<NewStringEvent, String> MIDDLE_STRING_TRANSLATOR =
            new EventTranslatorOneArg<NewStringEvent, String>() {
                @Override
                public void translateTo(NewStringEvent newStringEvent, long sequence, String newString) {
                    newStringEvent.setLast(false);
                    newStringEvent.setNewString(newString);
                }
            };

    private static final EventTranslatorOneArg<NewStringEvent, String> LAST_STRING_TRANSLATOR =
            new EventTranslatorOneArg<NewStringEvent, String>() {
                @Override
                public void translateTo(NewStringEvent newStringEvent, long sequence, String newString) {
                    newStringEvent.setLast(true);
                    newStringEvent.setNewString(newString);
                }
            };

}
