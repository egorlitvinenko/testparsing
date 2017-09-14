package org.egorlitvinenko.testdisruptor.clickhousestream.validators;

import java.util.function.Predicate;

/**
 * @author Egor Litvinenko
 */
public class DoubleCheckCharacters implements Predicate<String> {

    private static final char MINUS = '-', PLUS = '+', E = 'E', e = 'e', POINT = '.';

    @Override
    public boolean test(String s) {
        return plainCharAt(s);
    }

    public final static boolean plainCharAt(String s) {
        for (int i = 0; i < s.length(); ++i) {
            final char c = s.charAt(i);
            switch (c) {
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
                    break;
                case PLUS:
                case MINUS:
                    if (i > 1 && c != 'E' && c != 'e' || i == 0)
                        return false;
                    break;
                case E:
                case e:
                case POINT:
                    if (i == s.length() - 1 || i == 0)
                        return false;
                    break;
                default:
                    return false;
            }
        }
        return true;
    }

}
