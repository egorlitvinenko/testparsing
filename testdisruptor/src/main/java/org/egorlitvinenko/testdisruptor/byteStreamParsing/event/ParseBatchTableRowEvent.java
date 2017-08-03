package org.egorlitvinenko.testdisruptor.byteStreamParsing.event;

import org.egorlitvinenko.testdisruptor.byteStreamParsing.model.TableRow;
import org.egorlitvinenko.testdisruptor.byteStreamParsing.util.ColumnType;

/**
 * @author Egor Litvinenko
 */
public class ParseBatchTableRowEvent {

    private boolean end = false;
    private TableRow[] tableRows;
    private ColumnType type;
    private int size;

    public boolean isEnd() {
        return end;
    }

    public void setEnd(boolean end) {
        this.end = end;
    }

    public TableRow[] getTableRows() {
        return tableRows;
    }

    public void setTableRows(TableRow[] tableRows) {
        this.tableRows = tableRows;
    }

    public ColumnType getType() {
        return type;
    }

    public void setType(ColumnType type) {
        this.type = type;
    }

    public int getSize() {
        return size;
    }

    public void setSize(int size) {
        this.size = size;
    }


}
