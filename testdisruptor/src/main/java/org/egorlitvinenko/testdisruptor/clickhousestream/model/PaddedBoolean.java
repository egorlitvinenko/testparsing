package org.egorlitvinenko.testdisruptor.clickhousestream.model;

//import jdk.internal.vm.annotation.Contended;
//import sun.misc.Contended;

import org.openjdk.jol.info.ClassLayout;

/**
 * @author Egor Litvinenko
 */
public class PaddedBoolean {

//    @Contended
    public volatile long p0, p1, p2;
    public volatile long value;
    public volatile long q0, q1, q2;

    public PaddedBoolean() {
        this.value = 0;
    }

    public void set(boolean value) {
        this.value = value ? 1 : 0;
    }

    public boolean get() {
        return this.value == 1;
    }

    public static void main(String[] args) {
        System.out.println(ClassLayout.parseClass(PaddedBoolean.class).toPrintable());
    }


}
