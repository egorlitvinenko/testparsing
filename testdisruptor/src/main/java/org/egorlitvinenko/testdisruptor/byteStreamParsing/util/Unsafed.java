package org.egorlitvinenko.testdisruptor.byteStreamParsing.util;

import sun.misc.Unsafe;

import java.lang.reflect.Field;

/**
 * @author Egor Litvinenko
 */
public class Unsafed {

    private static final Unsafe UNSAFE = _getUnsafe();

    private static Unsafe _getUnsafe() throws RuntimeException {
        try {
            Field f = Unsafe.class.getDeclaredField("theUnsafe");
            f.setAccessible(true);
            Unsafe unsafe = (Unsafe) f.get(null);
            return unsafe;
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    public static Unsafe get() {
        return UNSAFE;
    }


}
