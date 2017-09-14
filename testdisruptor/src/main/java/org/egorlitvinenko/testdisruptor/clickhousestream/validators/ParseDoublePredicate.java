package org.egorlitvinenko.testdisruptor.clickhousestream.validators;

import java.util.function.Predicate;

/**
 * @author Egor Litvinenko
 */
public class ParseDoublePredicate implements Predicate<String> {

    @Override
    public boolean test(String s) {
        try {
            Double.parseDouble(s);
            return true;
        } catch (Exception e) {
            return false;
        }
    }
}
