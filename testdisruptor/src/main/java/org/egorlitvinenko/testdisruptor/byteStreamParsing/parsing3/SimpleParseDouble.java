package org.egorlitvinenko.testdisruptor.byteStreamParsing.parsing3;

import org.egorlitvinenko.testdisruptor.byteStreamParsing.model.TableRow;
import org.egorlitvinenko.testdisruptor.byteStreamParsing.model.group.States;

/**
 * @author Egor Litvinenko
 */
public class SimpleParseDouble implements TableRowInternalParsingStrategy {

    @Override
    public void parse(TableRow tableRow, int index) {
        try {
            tableRow.setDouble(
                    Double.parseDouble(tableRow.getDoubleString(index)),
                    index,
                    States.PARSED);
        } catch (NumberFormatException e) {
        }
    }

}
