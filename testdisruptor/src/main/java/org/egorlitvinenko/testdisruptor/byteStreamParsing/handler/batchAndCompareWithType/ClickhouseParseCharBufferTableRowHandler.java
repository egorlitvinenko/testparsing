package org.egorlitvinenko.testdisruptor.byteStreamParsing.handler.batchAndCompareWithType;

import org.egorlitvinenko.testdisruptor.byteStreamParsing.adapter.TableRowAndPrepareStatementAdapter;
import org.egorlitvinenko.testdisruptor.byteStreamParsing.event.ParseCharBufferTableRowEvent;
import org.egorlitvinenko.testdisruptor.byteStreamParsing.handler.AbstractClickhouseOneTableRowHandler;

/**
 * @author Egor Litvinenko
 */
public class ClickhouseParseCharBufferTableRowHandler extends AbstractClickhouseOneTableRowHandler<ParseCharBufferTableRowEvent> {

    public static int BATCH_SIZE = 15000;

    public ClickhouseParseCharBufferTableRowHandler(String insert,
                                                    TableRowAndPrepareStatementAdapter tableRowAndPrepareStatementAdapter) {
        super(insert, BATCH_SIZE, tableRowAndPrepareStatementAdapter);
    }

}
