package org.egorlitvinenko.testdisruptor.clickhousestream.model;

/**
 * @author Egor Litvinenko
 */
public interface Line {

    boolean endStream();

    String[] values();

    void markAsInvalid();

    void markAsFinished(int index);

    boolean isValid();

    boolean isFinished();

    void reset();

}
