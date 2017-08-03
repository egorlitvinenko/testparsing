package org.egorlitvinenko.testdisruptor.byteStreamParsing.parsing.api;

/**
 * @author Egor Litvinenko
 */
public interface ParsingStrategy<Result extends AbstractParserResult> {

    Result parse(String value);

}
