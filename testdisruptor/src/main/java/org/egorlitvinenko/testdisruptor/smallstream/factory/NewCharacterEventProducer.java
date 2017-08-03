package org.egorlitvinenko.testdisruptor.smallstream.factory;

import org.egorlitvinenko.testdisruptor.smallstream.event.NewByteEvent;
import com.lmax.disruptor.EventTranslatorOneArg;
import com.lmax.disruptor.RingBuffer;

import java.nio.ByteBuffer;

/**
 * @author Egor Litvinenko
 */
public class NewCharacterEventProducer {

    public static void onData(RingBuffer<NewByteEvent> ringBuffer, ByteBuffer byteBuffer) {
        ringBuffer.publishEvent(TRANSLATOR, byteBuffer);
    }

    public static void onData(RingBuffer<NewByteEvent> ringBuffer, Byte oneByte) {
        ringBuffer.publishEvent(BYTE_TRANSLATOR, oneByte);
    }

    private static final EventTranslatorOneArg<NewByteEvent, ByteBuffer> TRANSLATOR = new EventTranslatorOneArg<NewByteEvent, ByteBuffer>() {
        @Override
        public void translateTo(NewByteEvent event, long sequence, ByteBuffer byteBuffer) {
            event.setCharacter(byteBuffer.get(0));
        }
    };

    private static final EventTranslatorOneArg<NewByteEvent, Byte> BYTE_TRANSLATOR = new EventTranslatorOneArg<NewByteEvent, Byte>() {
        @Override
        public void translateTo(NewByteEvent event, long sequence, Byte oneByte) {
            event.setCharacter(oneByte);
        }
    };

}
