package org.egorlitvinenko.testdisruptor.clickhousestream.validators;

import java.util.function.Predicate;

/**
 * @author Egor Litvinenko
 */
public class SqlDateCheckCharacters implements Predicate<String> {

    private static final char MINUS = '-';

    @Override
    public boolean test(String s) {
        return testCharCopy(s);
    }

    public static boolean testCharCopy(String s) {
        char[] copy = new char[s.length()];
        s.getChars(0, s.length(), copy, 0);
        for (int i = 0; i < copy.length; ++i) {
            switch (copy[i]) {
                case '0':
                case '1':
                case '2':
                case '3':
                case '4':
                case '5':
                case '6':
                case '7':
                case '8':
                case '9':
                case MINUS:
                    break;
                default:
                    return false;
            }
        }
        return true;
    }

}
