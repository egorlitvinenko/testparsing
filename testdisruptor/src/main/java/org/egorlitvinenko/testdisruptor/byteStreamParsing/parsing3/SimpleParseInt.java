package org.egorlitvinenko.testdisruptor.byteStreamParsing.parsing3;

import org.egorlitvinenko.testdisruptor.byteStreamParsing.model.TableRow;
import org.egorlitvinenko.testdisruptor.byteStreamParsing.model.group.States;

/**
 * @author Egor Litvinenko
 */
public class SimpleParseInt implements TableRowInternalParsingStrategy {

    @Override
    public void parse(TableRow tableRow, int index) {
        try {
            tableRow.setInt32(
                    Integer.parseInt(tableRow.getInt32String(index)),
                    index,
                    States.PARSED);
        } catch (NumberFormatException e) {
        }
    }

}
