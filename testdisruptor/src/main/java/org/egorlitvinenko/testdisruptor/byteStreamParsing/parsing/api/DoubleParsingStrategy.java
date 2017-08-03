package org.egorlitvinenko.testdisruptor.byteStreamParsing.parsing.api;

/**
 * @author Egor Litvinenko
 */
public interface DoubleParsingStrategy extends ParsingStrategy<DoubleParserResult> {

    DoubleParserResult parse(String value);

}
