package org.egorlitvinenko.testdisruptor.byteStreamParsing.event;

import org.egorlitvinenko.testdisruptor.byteStreamParsing.model.TableRow;

/**
 * @author Egor Litvinenko
 */
public class ParseTableRowEvent {

    private boolean end = false;
    private TableRow tableRow;
    private int currentColumn;

    public boolean isEnd() {
        return end;
    }

    public void setEnd(boolean end) {
        this.end = end;
    }

    public TableRow getTableRow() {
        return tableRow;
    }

    public void setTableRow(TableRow tableRow) {
        this.tableRow = tableRow;
    }

    public int getCurrentColumn() {
        return currentColumn;
    }


    public void setCurrentColumn(int currentColumn) {
        this.currentColumn = currentColumn;
    }
}
