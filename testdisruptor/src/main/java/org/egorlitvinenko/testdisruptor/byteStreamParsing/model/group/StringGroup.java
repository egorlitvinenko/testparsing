package org.egorlitvinenko.testdisruptor.byteStreamParsing.model.group;

/**
 * @author Egor Litvinenko
 */
public class StringGroup  {

    public final int length;
    public final String[] values;

    public StringGroup(int length) {
        this.length = length;
        this.values = new String[length];
    }
}
