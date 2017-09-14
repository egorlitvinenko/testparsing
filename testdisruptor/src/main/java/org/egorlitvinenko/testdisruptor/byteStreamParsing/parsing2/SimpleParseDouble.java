package org.egorlitvinenko.testdisruptor.byteStreamParsing.parsing2;

import org.egorlitvinenko.testdisruptor.byteStreamParsing.model.PrimitiveTableRow;
import org.egorlitvinenko.testdisruptor.byteStreamParsing.model.group.States;

/**
 * @author Egor Litvinenko
 */
public class SimpleParseDouble implements InternalParsingStrategy {

    @Override
    public void parse(PrimitiveTableRow tableRow, int index) {
        try {
            tableRow.setDouble(
                    Double.parseDouble(tableRow.getDoubleString(index)),
                    index,
                    States.PARSED);
        } catch (NumberFormatException e) {
        }
    }

}
