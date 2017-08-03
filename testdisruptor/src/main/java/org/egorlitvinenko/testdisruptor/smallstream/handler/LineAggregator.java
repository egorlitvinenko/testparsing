package org.egorlitvinenko.testdisruptor.smallstream.handler;

import org.egorlitvinenko.testdisruptor.smallstream.event.NewCombinedStringLineEvent;
import org.egorlitvinenko.testdisruptor.smallstream.event.NewStringEvent;
import com.lmax.disruptor.EventHandler;
import com.lmax.disruptor.EventTranslatorOneArg;
import com.lmax.disruptor.RingBuffer;

/**
 * @author Egor Litvinenko
 */
public class LineAggregator implements EventHandler<NewStringEvent> {

    private final CombinedStringTranslator combinedStringTranslator;
    private final RingBuffer<NewCombinedStringLineEvent> ringBuffer;
    private final String[] line;
    private int counter;

    public LineAggregator(int lineLength, RingBuffer<NewCombinedStringLineEvent> ringBuffer) {
        this.line = new String[lineLength];
        this.ringBuffer = ringBuffer;
        this.combinedStringTranslator = new CombinedStringTranslator();
    }

    @Override
    public void onEvent(NewStringEvent event, long sequence, boolean endOfBatch) throws Exception {
        line[counter++] = event.getNewString();
        if (event.isLast()) {
            ringBuffer.publishEvent(combinedStringTranslator, line);
            counter = 0;
        }
    }

    public static class CombinedStringTranslator implements EventTranslatorOneArg<NewCombinedStringLineEvent, String[]> {

        @Override
        public void translateTo(NewCombinedStringLineEvent event, long sequence, String[] line) {
            event.setLine(line);
        }

    }

    private static class Values {
        final int length;
        final String[] line;

        public Values(String[] line, int length) {
            this.length = length;
            this.line = line;
        }

    }


}
