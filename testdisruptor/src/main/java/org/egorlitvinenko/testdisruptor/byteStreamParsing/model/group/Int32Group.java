package org.egorlitvinenko.testdisruptor.byteStreamParsing.model.group;

/**
 * @author Egor Litvinenko
 */
public class Int32Group extends BaseParsedGroup {

    public final int[] int32s;

    public Int32Group(int length) {
        super(length);
        this.int32s = new int[length];
    }

}
