package org.egorlitvinenko.testdisruptor.byteStreamParsing.publisher;

import org.egorlitvinenko.testdisruptor.byteStreamParsing.event.StringToParseInt32Event;
import org.egorlitvinenko.testdisruptor.byteStreamParsing.model.TableRow;
import com.lmax.disruptor.RingBuffer;

/**
 * @author Egor Litvinenko
 */
public class Int32ParsePublisher extends AbstractPublisher<TableRow, StringToParseInt32Event> {

    public Int32ParsePublisher(RingBuffer<StringToParseInt32Event> ringBuffer) {
        super(ringBuffer);
    }

}
