package org.egorlitvinenko.testdisruptor.byteStreamParsing.model;

/**
 * @author Egor Litvinenko
 */
public class SingleThreadRowIsProcessedStrategy implements RowIsProcessedStrategy {

    private final int expectedCount;

    private int counter;

    public SingleThreadRowIsProcessedStrategy(int expectedCount) {
        this.expectedCount = expectedCount;
        this.counter = 0;
    }

    @Override
    public int expectedCount() {
        return expectedCount;
    }

    @Override
    public void incrementProcessedElements() {
        counter++;
    }

    @Override
    public boolean isFinished() {
        return counter == expectedCount;
    }

    @Override
    public void reset() {
        counter = 0;
    }
}
