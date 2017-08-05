package org.egorlitvinenko.testdisruptor.byteStreamParsing.handler.compareWithType;

import org.egorlitvinenko.testdisruptor.byteStreamParsing.adapter.TableRowAndPrepareStatementAdapter;
import org.egorlitvinenko.testdisruptor.byteStreamParsing.event.ParsePacketTableRowEvent;
import org.egorlitvinenko.testdisruptor.byteStreamParsing.handler.AbstractClickhouseOneTableRowHandler;

/**
 * @author Egor Litvinenko
 */
public class ClickhouseParsePacketTableRowHandler extends AbstractClickhouseOneTableRowHandler<ParsePacketTableRowEvent> {

    public static int BATCH_SIZE = 15000;

    public ClickhouseParsePacketTableRowHandler(String insert,
                                                TableRowAndPrepareStatementAdapter tableRowAndPrepareStatementAdapter) {
        super(insert, BATCH_SIZE, tableRowAndPrepareStatementAdapter);
    }

}
