package org.egorlitvinenko.testdisruptor.byteStreamParsing.parsing.spi;

import org.egorlitvinenko.testdisruptor.byteStreamParsing.model.group.States;
import org.egorlitvinenko.testdisruptor.byteStreamParsing.parsing.api.Int32ParserResult;
import org.egorlitvinenko.testdisruptor.byteStreamParsing.parsing.api.Int32ParsingStrategy;
import org.springframework.util.StringUtils;

/**
 * @author Egor Litvinenko
 */
public class SimpleIntegerValueOf implements Int32ParsingStrategy {

    private static final Int32ParserResult FORMAT_ERROR = new Int32ParserResult(States.FORMAT_ERROR, Integer.MIN_VALUE);
    private static final Int32ParserResult IS_NULL = new Int32ParserResult(States.IS_NULL, Integer.MIN_VALUE);

    @Override
    public Int32ParserResult parse(String value) {
        try {
            return new Int32ParserResult(States.PARSED, Integer.parseInt(value));
        } catch (NumberFormatException e) {
            if (StringUtils.isEmpty(value)) {
                return IS_NULL;
            } else {
                return FORMAT_ERROR;
            }
        }
    }
}
