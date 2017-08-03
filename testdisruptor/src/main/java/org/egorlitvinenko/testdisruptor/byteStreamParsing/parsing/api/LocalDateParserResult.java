package org.egorlitvinenko.testdisruptor.byteStreamParsing.parsing.api;

import java.time.LocalDate;

/**
 * @author Egor Litvinenko
 */
public class LocalDateParserResult extends AbstractParserResult {

    public final LocalDate value;

    public LocalDateParserResult(LocalDate value, byte state) {
        super(state);
        this.value = value;
    }

    public LocalDateParserResult(LocalDate value) {
        this.value = value;
    }

}
