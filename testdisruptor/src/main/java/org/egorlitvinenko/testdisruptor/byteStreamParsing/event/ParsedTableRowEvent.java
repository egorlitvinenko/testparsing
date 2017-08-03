package org.egorlitvinenko.testdisruptor.byteStreamParsing.event;

import org.egorlitvinenko.testdisruptor.byteStreamParsing.model.TableRow;

/**
 * @author Egor Litvinenko
 */
public class ParsedTableRowEvent {

    private volatile TableRow tableRow;

    public TableRow getTableRow() {
        // TODO
        while (null == tableRow) {
            Thread.yield();
        }
        return tableRow;
    }

    public void setTableRow(TableRow tableRow) {
        this.tableRow = tableRow;
    }

}
