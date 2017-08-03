package org.egorlitvinenko.testdisruptor.byteStreamParsing.handler.compareWithType;

import org.egorlitvinenko.testdisruptor.byteStreamParsing.event.ParsePacketTableRowEvent;
import org.egorlitvinenko.testdisruptor.byteStreamParsing.model.TableRow;
import org.egorlitvinenko.testdisruptor.byteStreamParsing.parsing.api.AbstractParserResult;
import org.egorlitvinenko.testdisruptor.byteStreamParsing.parsing.api.ParsingStrategy;
import org.egorlitvinenko.testdisruptor.byteStreamParsing.util.ColumnType;
import com.lmax.disruptor.EventHandler;

/**
 * @author Egor Litvinenko
 */
public abstract class AbstractParsePacketHandler<
        Result extends AbstractParserResult,
        Parser extends ParsingStrategy<Result>> implements EventHandler<ParsePacketTableRowEvent> {

    protected final int[] myColumns;
    protected final Parser parser;
    protected final ColumnType myColumnType;

    public AbstractParsePacketHandler(Parser parser, int[] myColumns, ColumnType myColumnType) {
        this.myColumns = myColumns;
        this.myColumnType = myColumnType;
        this.parser = parser;
    }

    @Override
    public void onEvent(ParsePacketTableRowEvent event, long sequence, boolean endOfBatch) throws Exception {
        if (!event.isEnd() && myColumnType.equals(event.getType())) {
            for (int i = 0; i < myColumns.length; ++i) {
                Result result = parser.parse(getStringValue(event.getTableRow(), myColumns[i]));
                setValue(event.getTableRow(), myColumns[i], result);
            }
            finished(event.getTableRow());
        }
    }

    protected abstract void finished(TableRow tableRow);

    protected abstract String getStringValue(TableRow tableRow, int index);

    protected abstract void setValue(TableRow tableRow, int index, Result result);

}
