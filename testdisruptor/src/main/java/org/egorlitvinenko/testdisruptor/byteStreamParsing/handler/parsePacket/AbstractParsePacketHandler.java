package org.egorlitvinenko.testdisruptor.byteStreamParsing.handler.parsePacket;

import com.lmax.disruptor.EventHandler;
import org.egorlitvinenko.testdisruptor.byteStreamParsing.event.ParsePacketEvent;
import org.egorlitvinenko.testdisruptor.byteStreamParsing.model.TableRow;
import org.egorlitvinenko.testdisruptor.byteStreamParsing.parsing.api.AbstractParserResult;
import org.egorlitvinenko.testdisruptor.byteStreamParsing.parsing.api.ParsingStrategy;

/**
 * @author Egor Litvinenko
 */
public abstract class AbstractParsePacketHandler<
        Result extends AbstractParserResult,
        Parser extends ParsingStrategy<Result>> implements EventHandler<ParsePacketEvent> {

    protected final int[] myColumns;
    protected final Parser parser;

    public AbstractParsePacketHandler(Parser parser, int[] myColumns) {
        this.myColumns = myColumns;
        this.parser = parser;
    }

    @Override
    public void onEvent(ParsePacketEvent event, long sequence, boolean endOfBatch) throws Exception {
        if (!event.isEnd() && hasMyType(event)) {
            for (int i = 0; i < myColumns.length; ++i) {
                Result result = parser.parse(getStringValue(event, myColumns[i]));
                setValue(event, myColumns[i], result);
            }
            finished(event);
        }
    }

    protected abstract boolean hasMyType(ParsePacketEvent event);

    protected abstract void finished(TableRow tableRow);

    protected abstract String getStringValue(TableRow tableRow, int index);

    protected abstract void setValue(TableRow tableRow, int index, Result result);

}
