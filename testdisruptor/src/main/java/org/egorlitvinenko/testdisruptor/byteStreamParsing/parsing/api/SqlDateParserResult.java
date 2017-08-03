package org.egorlitvinenko.testdisruptor.byteStreamParsing.parsing.api;

/**
 * @author Egor Litvinenko
 */
public class SqlDateParserResult extends AbstractParserResult {

    public final java.sql.Date value;

    public SqlDateParserResult(java.sql.Date value, byte state) {
        super(state);
        this.value = value;
    }

    public SqlDateParserResult(java.sql.Date value) {
        this.value = value;
    }

}
