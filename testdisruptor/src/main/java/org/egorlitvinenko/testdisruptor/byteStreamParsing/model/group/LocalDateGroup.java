package org.egorlitvinenko.testdisruptor.byteStreamParsing.model.group;

import java.time.LocalDate;

/**
 * @author Egor Litvinenko
 */
public class LocalDateGroup extends BaseParsedGroup {

    public LocalDate[] dates;

    public LocalDateGroup(int length) {
        super(length);
        this.dates = new LocalDate[length];
    }

}
