package org.egorlitvinenko.testdisruptor.byteStreamParsing.handler.splittedByEvent;

import org.egorlitvinenko.testdisruptor.byteStreamParsing.event.AbstractStringToParseEvent;
import org.egorlitvinenko.testdisruptor.byteStreamParsing.event.ParsedTableRowEvent;
import org.egorlitvinenko.testdisruptor.byteStreamParsing.model.TableRow;
import org.egorlitvinenko.testdisruptor.byteStreamParsing.parsing.api.AbstractParserResult;
import org.egorlitvinenko.testdisruptor.byteStreamParsing.parsing.api.ParsingStrategy;
import com.lmax.disruptor.EventHandler;
import com.lmax.disruptor.EventTranslatorOneArg;
import com.lmax.disruptor.RingBuffer;

/**
 * @author Egor Litvinenko
 */
public abstract class AbstractParseHandler<
        Event extends AbstractStringToParseEvent<TableRow>,
        Result extends AbstractParserResult,
        Parser extends ParsingStrategy<Result>> implements EventHandler<Event> {

    private volatile int counter = 0;

    protected final Parser parser;
    protected final RingBuffer<ParsedTableRowEvent> ringBuffer;

    public AbstractParseHandler(Parser parser, RingBuffer<ParsedTableRowEvent> ringBuffer) {
        this.parser = parser;
        this.ringBuffer = ringBuffer;
    }

    @Override
    public void onEvent(Event event, long sequence, boolean endOfBatch) throws Exception {
        counter++;
        Result result = parser.parse(getStringValue(event.getTableRow(), event.getIndex()));
        setValue(event.getTableRow(), event.getIndex(), result);
        synchronized (ringBuffer) {
            this.ringBuffer.publishEvent(TRANSLATOR, event.getTableRow());
        }
    }

    protected abstract String getStringValue(TableRow tableRow, int index);

    protected abstract void setValue(TableRow tableRow, int index, Result result);

    static class Translator implements EventTranslatorOneArg<ParsedTableRowEvent, TableRow> {
        int counter2 = 0;

        @Override
        public void translateTo(ParsedTableRowEvent event, long sequence, TableRow arg0) {
            counter2++;
            event.setTableRow(arg0);
        }
    }

    private static Translator TRANSLATOR = new Translator();

    public int getCounter() {
        return counter;
    }

    public int getCounter2() {
        return TRANSLATOR.counter2;
    }
}
