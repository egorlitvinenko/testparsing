package org.egorlitvinenko.testdisruptor.byteStreamParsing.parsing.spi;

import org.egorlitvinenko.testdisruptor.byteStreamParsing.model.group.States;
import org.egorlitvinenko.testdisruptor.byteStreamParsing.parsing.api.LocalDateParserResult;
import org.egorlitvinenko.testdisruptor.byteStreamParsing.parsing.api.LocalDateParserStrategy;
import org.springframework.util.StringUtils;

import java.time.LocalDate;
import java.time.format.DateTimeFormatter;

/**
 * @author Egor Litvinenko
 */
public class SimpleIsoLocalDateParser implements LocalDateParserStrategy {

    private static DateTimeFormatter DATE_FORMATTER = DateTimeFormatter.ISO_LOCAL_DATE;

    private static final LocalDateParserResult IS_NULL = new LocalDateParserResult(LocalDate.MIN, States.IS_NULL);
    private static final LocalDateParserResult FORMAT_ERROR = new LocalDateParserResult(LocalDate.MIN, States.FORMAT_ERROR);

    @Override
    public LocalDateParserResult parse(String value) {
        try {
            return new LocalDateParserResult(LocalDate.from(DATE_FORMATTER.parse(value)));
        } catch (Exception e) {
            if (StringUtils.isEmpty(value)) {
                return IS_NULL;
            } else {
                return FORMAT_ERROR;
            }
        }
    }

}
