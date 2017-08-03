package org.egorlitvinenko.testdisruptor.byteStreamParsing.publisher;

import org.egorlitvinenko.testdisruptor.byteStreamParsing.event.StringToParseDoubleEvent;
import org.egorlitvinenko.testdisruptor.byteStreamParsing.model.TableRow;
import com.lmax.disruptor.RingBuffer;

/**
 * @author Egor Litvinenko
 */
public class DoubleParsePublisher extends AbstractPublisher<TableRow, StringToParseDoubleEvent> {

    public DoubleParsePublisher(RingBuffer<StringToParseDoubleEvent> ringBuffer) {
        super(ringBuffer);
    }

}
