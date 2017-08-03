package org.egorlitvinenko.testdisruptor.byteStreamParsing.model;

import org.egorlitvinenko.testdisruptor.byteStreamParsing.model.group.BaseParsedGroup;

import java.sql.Date;

/**
 * @author Egor Litvinenko
 */
public class SqlDateGroup extends BaseParsedGroup {

    public Date[] dates;

    public SqlDateGroup(int length) {
        super(length);
        this.dates = new Date[length];
    }

}
