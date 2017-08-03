package org.egorlitvinenko.testdisruptor.byteStreamParsing.model;

/**
 * @author Egor Litvinenko
 */
public class SyncRowIsProcessedStrategy implements RowIsProcessedStrategy {

    private final int expectedCount;

    private int counter;

    public SyncRowIsProcessedStrategy(int expectedCount) {
        this.expectedCount = expectedCount;
        this.counter = 0;
    }

    @Override
    public int expectedCount() {
        return expectedCount;
    }

    @Override
    public void incrementProcessedElements() {
        synchronized (this) {
            counter++;
        }
    }

    @Override
    public boolean isFinished() {
        synchronized (this) {
            return counter == expectedCount;
        }
    }

    @Override
    public void reset() {
        synchronized (this) {
            counter = 0;
        }
    }
}
