package org.egorlitvinenko.testdisruptor.byteStreamParsing.parsing;

/**
 * @author Egor Litvinenko
 */
public class PositionedIsoDateParser {

    public int year, month, day;

    public void parse(String value) throws Exception {
        year = four(value);
        month = two(value, 5);
        day = two(value, 8);
    }

    private static final int TWO_SUBTRACT = '0' * 11;

    private static int two(final String s, final int from) {
        return s.charAt(from) * 10 + s.charAt(from + 1) - TWO_SUBTRACT;
    }

    private static int four(final String s) {
        return 100 * two(s, 0) + two(s, 2);
    }

}
