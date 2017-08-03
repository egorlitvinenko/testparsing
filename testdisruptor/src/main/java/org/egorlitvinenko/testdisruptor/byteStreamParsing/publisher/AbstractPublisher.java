package org.egorlitvinenko.testdisruptor.byteStreamParsing.publisher;

import org.egorlitvinenko.testdisruptor.byteStreamParsing.event.AbstractStringToParseEvent;
import org.egorlitvinenko.testdisruptor.byteStreamParsing.model.TableRow;
import com.lmax.disruptor.EventTranslatorTwoArg;
import com.lmax.disruptor.RingBuffer;

/**
 * @author Egor Litvinenko
 */
public class AbstractPublisher<
        TableRowType extends TableRow,
        Event extends AbstractStringToParseEvent<TableRowType>>
        implements EventTranslatorTwoArg<Event, TableRowType, Integer> {

    private final RingBuffer<Event> ringBuffer;

    public AbstractPublisher(RingBuffer<Event> ringBuffer) {
        this.ringBuffer = ringBuffer;
    }

    public void publish(TableRowType tableRow, Integer index) {
        this.ringBuffer.publishEvent(this, tableRow, index);
    }

    @Override
    public void translateTo(Event event, long sequence, TableRowType tableRow, Integer index) {
        event.setTableRow(tableRow);
        event.setIndex(index);
    }

}
