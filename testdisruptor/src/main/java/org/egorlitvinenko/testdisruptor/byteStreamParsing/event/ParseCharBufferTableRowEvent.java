package org.egorlitvinenko.testdisruptor.byteStreamParsing.event;

import org.egorlitvinenko.testdisruptor.byteStreamParsing.disruptor.ParseCharBufferCsvDisruptor;
import org.egorlitvinenko.testdisruptor.byteStreamParsing.model.Line;
import org.egorlitvinenko.testdisruptor.byteStreamParsing.model.SimpleLine;
import org.egorlitvinenko.testdisruptor.byteStreamParsing.model.TableRow;
import org.egorlitvinenko.testdisruptor.byteStreamParsing.util.ColumnType;

/**
 * @author Egor Litvinenko
 */
public class ParseCharBufferTableRowEvent implements Line, HasOneTableRowEvent {

    private SimpleLine line;
    private boolean end = false;
    private TableRow tableRow;
    private ColumnType type;

    public ParseCharBufferTableRowEvent(int size) {
        this.line = new SimpleLine(size);
    }

    public boolean isEnd() {
        return end;
    }

    public String getValue(int index) {
        return line.getValue(index);
    }

    public void setEnd(boolean end) {
        this.end = end;
    }

    public void setChars(char[] buffer) {
        line.setChars(buffer);
    }

    public void setMap(int[][] map) {
        line.setMap(map);
    }

    public TableRow getTableRow() {
        return tableRow;
    }

    public void setTableRow(TableRow tableRow) {
        this.tableRow = tableRow;
    }

    public ColumnType getType() {
        return type;
    }

    public void setType(ColumnType type) {
        this.type = type;
    }
}
