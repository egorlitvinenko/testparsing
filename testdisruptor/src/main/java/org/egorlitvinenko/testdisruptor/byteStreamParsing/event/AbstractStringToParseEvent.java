package org.egorlitvinenko.testdisruptor.byteStreamParsing.event;

import org.egorlitvinenko.testdisruptor.byteStreamParsing.model.TableRow;

/**
 * @author Egor Litvinenko
 */
public class AbstractStringToParseEvent<TableRowType extends TableRow> {

    private TableRowType tableRow;
    private int index;

    public TableRowType getTableRow() {
        return tableRow;
    }

    public void setTableRow(TableRowType tableRow) {
        this.tableRow = tableRow;
    }

    public int getIndex() {
        return index;
    }

    public void setIndex(int index) {
        this.index = index;
    }
}
