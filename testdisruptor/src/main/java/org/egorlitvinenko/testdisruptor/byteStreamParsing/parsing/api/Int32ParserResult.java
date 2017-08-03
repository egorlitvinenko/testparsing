package org.egorlitvinenko.testdisruptor.byteStreamParsing.parsing.api;

/**
 * @author Egor Litvinenko
 */
public class Int32ParserResult extends AbstractParserResult {

    public final int value;

    public Int32ParserResult(byte state, int value) {
        super(state);
        this.value = value;
    }

    public Int32ParserResult(int value) {
        this.value = value;
    }
}
