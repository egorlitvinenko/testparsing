package org.egorlitvinenko.testdisruptor.byteStreamParsing.parsing2;

import org.egorlitvinenko.testdisruptor.byteStreamParsing.model.PrimitiveTableRow;
import org.egorlitvinenko.testdisruptor.byteStreamParsing.model.group.States;
import org.egorlitvinenko.testdisruptor.byteStreamParsing.parsing.PositionedIsoDateParser;

/**
 * @author Egor Litvinenko
 */
public class SqlDateParser implements InternalParsingStrategy {

    private final PositionedIsoDateParser positionedIsoDateParser = new PositionedIsoDateParser();

    @Override
    public void parse(PrimitiveTableRow tableRow, int index) {
        try {
            positionedIsoDateParser.parse(tableRow.getSqlDateString(index));
            tableRow.setSqlDate(positionedIsoDateParser.year - 1900,
                    positionedIsoDateParser.month - 1,
                    positionedIsoDateParser.day, index, States.PARSED);
        } catch (Exception e) {
        }
    }

}
