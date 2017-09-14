package org.egorlitvinenko.testdisruptor.byteStreamParsing.parsing3;

import org.egorlitvinenko.testdisruptor.byteStreamParsing.model.TableRow;
import org.egorlitvinenko.testdisruptor.byteStreamParsing.model.group.States;
import org.egorlitvinenko.testdisruptor.byteStreamParsing.parsing.PositionedIsoDateParser;

import java.sql.Date;

/**
 * @author Egor Litvinenko
 */
public class SqlDateParser implements TableRowInternalParsingStrategy {

    private final PositionedIsoDateParser positionedIsoDateParser = new PositionedIsoDateParser();

    @Override
    public void parse(TableRow tableRow, int index) {
        try {
            positionedIsoDateParser.parse(tableRow.getSqlDateString(index));
            tableRow.setSqlDate(new Date(positionedIsoDateParser.year - 1900,
                    positionedIsoDateParser.month - 1,
                    positionedIsoDateParser.day), index, States.PARSED);
        } catch (Exception e) {
            tableRow.setSqlDate(null, index, States.FORMAT_ERROR);
        }
    }

}
