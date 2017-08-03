package org.egorlitvinenko.testdisruptor.byteStreamParsing.parsing.spi;

import org.egorlitvinenko.testdisruptor.byteStreamParsing.model.group.States;
import org.egorlitvinenko.testdisruptor.byteStreamParsing.parsing.api.DoubleParserResult;
import org.egorlitvinenko.testdisruptor.byteStreamParsing.parsing.api.DoubleParsingStrategy;
import org.springframework.util.StringUtils;

/**
 * @author Egor Litvinenko
 */
public class SimpleDoubleValueOf implements DoubleParsingStrategy {

    private static final DoubleParserResult IS_NULL = new DoubleParserResult(States.IS_NULL, Double.MIN_VALUE);
    private static final DoubleParserResult FORMAT_ERROR = new DoubleParserResult(States.FORMAT_ERROR, Double.MIN_VALUE);

    @Override
    public DoubleParserResult parse(String value) {
        try {
            return new DoubleParserResult(Double.parseDouble(value));
        } catch (NumberFormatException e) {
            if (StringUtils.isEmpty(value)) {
                return IS_NULL;
            } else {
                return FORMAT_ERROR;
            }
        }
    }

}
