package org.egorlitvinenko.testdisruptor.byteStreamParsing.handler.splittedByColumn;

import org.egorlitvinenko.testdisruptor.byteStreamParsing.event.ParseTableRowEvent;
import org.egorlitvinenko.testdisruptor.byteStreamParsing.model.TableRow;
import org.egorlitvinenko.testdisruptor.byteStreamParsing.parsing.api.AbstractParserResult;
import org.egorlitvinenko.testdisruptor.byteStreamParsing.parsing.api.ParsingStrategy;
import com.lmax.disruptor.EventHandler;

/**
 * @author Egor Litvinenko
 */
public abstract class AbstractParseHandler2<
        Result extends AbstractParserResult,
        Parser extends ParsingStrategy<Result>> implements EventHandler<ParseTableRowEvent> {

    protected final boolean[] myColumns;
    protected final Parser parser;

    public AbstractParseHandler2(Parser parser, boolean[] myColumns) {
        this.myColumns = myColumns;
        this.parser = parser;
    }

    @Override
    public void onEvent(ParseTableRowEvent event, long sequence, boolean endOfBatch) throws Exception {
        if (!event.isEnd() && myColumns[event.getCurrentColumn()]) {
            Result result = parser.parse(getStringValue(event.getTableRow(), event.getCurrentColumn()));
            setValue(event.getTableRow(), event.getCurrentColumn(), result);
        }
    }

    protected abstract String getStringValue(TableRow tableRow, int index);

    protected abstract void setValue(TableRow tableRow, int index, Result result);

}
