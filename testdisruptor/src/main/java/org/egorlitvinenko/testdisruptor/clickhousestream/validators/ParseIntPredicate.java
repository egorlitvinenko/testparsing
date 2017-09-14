package org.egorlitvinenko.testdisruptor.clickhousestream.validators;

import java.util.function.Predicate;

/**
 * @author Egor Litvinenko
 */
public class ParseIntPredicate implements Predicate<String> {

    @Override
    public boolean test(String s) {
        try {
            Integer.parseInt(s);
            return true;
        } catch (Exception e) {
            return false;
        }
    }
}
