package org.egorlitvinenko.testdisruptor.byteStreamParsing.model;

/**
 * @author Egor Litvinenko
 */
public interface RowIsProcessedStrategy {

    int expectedCount();

    void incrementProcessedElements();

    boolean isFinished();

    void reset();

}
