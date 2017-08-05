package org.egorlitvinenko.testdisruptor.byteStreamParsing.handler.charBufferAndcompareWithType;

import com.lmax.disruptor.EventHandler;
import org.egorlitvinenko.testdisruptor.byteStreamParsing.event.ParseCharBufferTableRowEvent;
import org.egorlitvinenko.testdisruptor.byteStreamParsing.event.ParsePacketTableRowEvent;
import org.egorlitvinenko.testdisruptor.byteStreamParsing.model.TableRow;
import org.egorlitvinenko.testdisruptor.byteStreamParsing.parsing.api.AbstractParserResult;
import org.egorlitvinenko.testdisruptor.byteStreamParsing.parsing.api.ParsingStrategy;
import org.egorlitvinenko.testdisruptor.byteStreamParsing.util.ColumnType;

/**
 * @author Egor Litvinenko
 */
public abstract class AbstractParseCharBufferHandler<
        Result extends AbstractParserResult,
        Parser extends ParsingStrategy<Result>> implements EventHandler<ParseCharBufferTableRowEvent> {

    protected final int[] myColumns;
    protected final Parser parser;
    protected final ColumnType myColumnType;

    public AbstractParseCharBufferHandler(Parser parser, int[] myColumns, ColumnType myColumnType) {
        this.myColumns = myColumns;
        this.myColumnType = myColumnType;
        this.parser = parser;
    }

    @Override
    public void onEvent(ParseCharBufferTableRowEvent event, long sequence, boolean endOfBatch) throws Exception {
        if (!event.isEnd() && myColumnType.equals(event.getType())) {
            for (int i = 0; i < myColumns.length; ++i) {
                String value = event.getValue(myColumns[i]);
                Result result = parser.parse(value);
                setValue(event.getTableRow(), myColumns[i], result);
            }
            finished(event.getTableRow());
        }
    }

    protected abstract void finished(TableRow tableRow);

    protected abstract void setValue(TableRow tableRow, int index, Result result);

}
