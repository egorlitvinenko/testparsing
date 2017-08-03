package org.egorlitvinenko.testdisruptor.byteStreamParsing.parsing.api;

/**
 * @author Egor Litvinenko
 */
public interface Int32ParsingStrategy extends ParsingStrategy<Int32ParserResult> {

    Int32ParserResult parse(String value);

}
