package org.egorlitvinenko.testdisruptor.byteStreamParsing.parsing.spi;

import org.egorlitvinenko.testdisruptor.byteStreamParsing.model.group.States;
import org.egorlitvinenko.testdisruptor.byteStreamParsing.parsing.api.SqlDateParserResult;
import org.egorlitvinenko.testdisruptor.byteStreamParsing.parsing.api.SqlDateParserStrategy;
import org.springframework.util.StringUtils;

import java.sql.Date;
import java.time.LocalDate;

/**
 * @author Egor Litvinenko
 */
public class PositionedIsoSqlDateParser implements SqlDateParserStrategy {

    private static final SqlDateParserResult IS_NULL = new SqlDateParserResult(Date.valueOf(LocalDate.MIN), States.IS_NULL);
    private static final SqlDateParserResult FORMAT_ERROR = new SqlDateParserResult(Date.valueOf(LocalDate.MIN), States.FORMAT_ERROR);

    @Override
    public SqlDateParserResult parse(String value) {
        try {
            return new SqlDateParserResult(new Date(
                    four(value) - 1900,
                    two(value, 5) - 1,
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
