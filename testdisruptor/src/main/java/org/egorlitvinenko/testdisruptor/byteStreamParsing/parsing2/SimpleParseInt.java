package org.egorlitvinenko.testdisruptor.byteStreamParsing.parsing2;

import org.egorlitvinenko.testdisruptor.byteStreamParsing.model.PrimitiveTableRow;
import org.egorlitvinenko.testdisruptor.byteStreamParsing.model.group.States;

/**
 * @author Egor Litvinenko
 */
public class SimpleParseInt implements InternalParsingStrategy {

    @Override
    public void parse(PrimitiveTableRow tableRow, int index) {
        try {
            tableRow.setInt32(
                    Integer.parseInt(tableRow.getInt32String(index)),
                    index,
                    States.PARSED);
        } catch (NumberFormatException e) {
        }
    }

}
