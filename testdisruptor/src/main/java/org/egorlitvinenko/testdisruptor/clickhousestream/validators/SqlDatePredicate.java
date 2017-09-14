package org.egorlitvinenko.testdisruptor.clickhousestream.validators;

import org.egorlitvinenko.testdisruptor.byteStreamParsing.parsing.PositionedIsoDateParser;

import java.util.function.Predicate;

/**
 * @author Egor Litvinenko
 */
public class SqlDatePredicate implements Predicate<String> {

    private final PositionedIsoDateParser positionedIsoDateParser = new PositionedIsoDateParser();

    @Override
    public boolean test(String s) {
        try {
            positionedIsoDateParser.parse(s);
            return true;
        } catch (Exception e) {
            return false;
        }
    }

}
