package org.egorlitvinenko.testdisruptor.byteStreamParsing.publisher;

import org.egorlitvinenko.testdisruptor.byteStreamParsing.event.StringToParseLocalDateEvent;
import org.egorlitvinenko.testdisruptor.byteStreamParsing.model.TableRow;
import com.lmax.disruptor.RingBuffer;

/**
 * @author Egor Litvinenko
 */
public class LocalDateParsePublisher extends AbstractPublisher<TableRow, StringToParseLocalDateEvent> {

    public LocalDateParsePublisher(RingBuffer<StringToParseLocalDateEvent> ringBuffer) {
        super(ringBuffer);
    }

}
