package org.egorlitvinenko.testdisruptor.byteStreamParsing.parsing.api;

import org.egorlitvinenko.testdisruptor.byteStreamParsing.model.group.States;

/**
 * @author Egor Litvinenko
 */
public class AbstractParserResult {

    public final byte state;

    public AbstractParserResult(byte state) {
        this.state = state;
    }

    public AbstractParserResult() {
        this.state = States.PARSED;
    }

}
