package org.egorlitvinenko.testdisruptor.byteStreamParsing.parsing.api;

/**
 * @author Egor Litvinenko
 */
public class DoubleParserResult extends AbstractParserResult {

    public final double value;

    public DoubleParserResult(byte state, double value) {
        super(state);
        this.value = value;
    }

    public DoubleParserResult(double value) {
        this.value = value;
    }

}
