package org.egorlitvinenko.testdisruptor.byteStreamParsing.parsing3;

import org.egorlitvinenko.testdisruptor.byteStreamParsing.model.TableRow;

/**
 * @author Egor Litvinenko
 */
public interface TableRowInternalParsingStrategy {

    void parse(TableRow tableRow, int index);

}
