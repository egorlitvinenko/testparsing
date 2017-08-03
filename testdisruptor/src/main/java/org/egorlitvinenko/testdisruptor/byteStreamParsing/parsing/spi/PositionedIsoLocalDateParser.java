package org.egorlitvinenko.testdisruptor.byteStreamParsing.parsing.spi;

import org.egorlitvinenko.testdisruptor.byteStreamParsing.model.group.States;
import org.egorlitvinenko.testdisruptor.byteStreamParsing.parsing.api.LocalDateParserResult;
import org.egorlitvinenko.testdisruptor.byteStreamParsing.parsing.api.LocalDateParserStrategy;
import org.springframework.util.StringUtils;

import java.time.LocalDate;

/**
 * @author Egor Litvinenko
 */
public class PositionedIsoLocalDateParser implements LocalDateParserStrategy {

    private static final LocalDateParserResult IS_NULL = new LocalDateParserResult(LocalDate.MIN, States.IS_NULL);
    private static final LocalDateParserResult FORMAT_ERROR = new LocalDateParserResult(LocalDate.MIN, States.FORMAT_ERROR);

    @Override
    public LocalDateParserResult parse(String value) {
        try {
            return new LocalDateParserResult(LocalDate.of(
                    four(value),
                    two(value, 5),
                    two(value, 8)));
        } catch (Exception e) {
            if (StringUtils.isEmpty(value)) {
                return IS_NULL;
            } else {
                return FORMAT_ERROR;
            }
        }
    }

    private static final int TWO_SUBTRACT = '0' * 11;

    private static int two(final String s, final int from) {
        return s.charAt(from) * 10 + s.charAt(from + 1) - TWO_SUBTRACT;
    }

    private static int four(final String s) {
        return 100 * two(s, 0) + two(s, 2);
    }

}
