package org.egorlitvinenko.testdisruptor.clickhousestream;

/**
 * @author Egor Litvinenko
 */
public class Preconditions {

    public static boolean isNullOrEmpty(String value) {
        return null == value || value.isEmpty();
    }

}
