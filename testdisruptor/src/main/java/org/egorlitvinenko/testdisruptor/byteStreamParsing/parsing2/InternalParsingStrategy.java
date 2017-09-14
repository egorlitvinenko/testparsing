package org.egorlitvinenko.testdisruptor.byteStreamParsing.parsing2;

import org.egorlitvinenko.testdisruptor.byteStreamParsing.model.PrimitiveTableRow;

/**
 * @author Egor Litvinenko
 */
public interface InternalParsingStrategy {

    void parse(PrimitiveTableRow tableRow, int index);

}
