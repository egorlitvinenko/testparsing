package org.egorlitvinenko.testdisruptor.byteStreamParsing.handler.batchAndCompareWithType;

import org.egorlitvinenko.testdisruptor.byteStreamParsing.event.ParseBatchTableRowEvent;
import org.egorlitvinenko.testdisruptor.byteStreamParsing.model.TableRow;
import org.egorlitvinenko.testdisruptor.byteStreamParsing.parsing.api.AbstractParserResult;
import org.egorlitvinenko.testdisruptor.byteStreamParsing.parsing.api.ParsingStrategy;
import org.egorlitvinenko.testdisruptor.byteStreamParsing.util.ColumnType;
import com.lmax.disruptor.EventHandler;

/**
 * @author Egor Litvinenko
 */
public abstract class AbstractParseBatchHandler<
        Result extends AbstractParserResult,
        Parser extends ParsingStrategy<Result>> implements EventHandler<ParseBatchTableRowEvent> {

    protected final int[] myColumns;
    protected final Parser parser;
    protected final ColumnType myColumnType;

    public AbstractParseBatchHandler(Parser parser, int[] myColumns, ColumnType myColumnType) {
        this.myColumns = myColumns;
        this.myColumnType = myColumnType;
        this.parser = parser;
    }

    @Override
    public void onEvent(ParseBatchTableRowEvent event, long sequence, boolean endOfBatch) throws Exception {
        if (!event.isEnd() && myColumnType.equals(event.getType())) {
            for (int j = 0; j < event.getSize(); j++) {
                for (int i = 0; i < myColumns.length; ++i) {
                    Result result = parser.parse(getStringValue(event.getTableRows()[j], myColumns[i]));
                    setValue(event.getTableRows()[j], myColumns[i], result);
                }
                finished(event.getTableRows()[j]);
            }
        }
    }

    protected abstract void finished(TableRow tableRow);

    protected abstract String getStringValue(TableRow tableRow, int index);

    protected abstract void setValue(TableRow tableRow, int index, Result result);

}
