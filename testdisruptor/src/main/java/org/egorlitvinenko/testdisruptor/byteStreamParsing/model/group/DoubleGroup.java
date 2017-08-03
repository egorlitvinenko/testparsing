package org.egorlitvinenko.testdisruptor.byteStreamParsing.model.group;

/**
 * @author Egor Litvinenko
 */
public class DoubleGroup extends BaseParsedGroup {

    public final double[] doubles;

    public DoubleGroup(int length) {
        super(length);
        this.doubles = new double[length];
    }

}
