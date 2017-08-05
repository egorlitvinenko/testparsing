package org.egorlitvinenko.testdisruptor.byteStreamParsing.handler.parsePacket;

import org.egorlitvinenko.testdisruptor.byteStreamParsing.event.ParsePacketEvent;
import org.egorlitvinenko.testdisruptor.byteStreamParsing.model.TableRow;
import org.egorlitvinenko.testdisruptor.byteStreamParsing.parsing.api.SqlDateParserResult;
import org.egorlitvinenko.testdisruptor.byteStreamParsing.parsing.api.SqlDateParserStrategy;
import org.egorlitvinenko.testdisruptor.byteStreamParsing.util.ColumnType;

/**
 * @author Egor Litvinenko
 */
public class StringToSqlDateParsePacketHandler extends AbstractParsePacketHandler<
        SqlDateParserResult,
        SqlDateParserStrategy> {

    public StringToSqlDateParsePacketHandler(SqlDateParserStrategy parsingStrategy,
                                             int[] myColumns) {
        super(parsingStrategy, myColumns);
    }

    @Override
    protected boolean hasMyType(ParsePacketEvent event) {
        return event.hasSqlDates();
    }

    @Override
    protected void finished(TableRow tableRow) {
        tableRow.setSqlDateIsFinished();
    }

    @Override
    protected String getStringValue(TableRow tableRow, int index) {
        return tableRow.getSqlDateString(index);
    }

    @Override
    protected void setValue(TableRow tableRow, int index, SqlDateParserResult result) {
        tableRow.setSqlDate(result.value, index, result.state);
    }
}
