package org.egorlitvinenko.testdisruptor.byteStreamParsing.model.group;

import org.egorlitvinenko.testdisruptor.byteStreamParsing.model.Format;

/**
 * @author Egor Litvinenko
 */
public class BaseParsedGroup extends StringGroup {

    public final Format[] formats;

    public BaseParsedGroup(int length) {
        super(length);
        this.formats = new Format[length];
    }

}
