package org.egorlitvinenko.testdisruptor.byteStreamParsing.factory;

import org.egorlitvinenko.testdisruptor.byteStreamParsing.model.RowIsProcessedStrategy;
import org.egorlitvinenko.testdisruptor.byteStreamParsing.model.SyncRowIsProcessedStrategy;

/**
 * @author Egor Litvinenko
 */
public class SyncRowIsProcessedStrategyFactory implements RowIsProcessedStrategyFactory {

    private final int expectedCount;

    public SyncRowIsProcessedStrategyFactory(int expectedCount) {
        this.expectedCount = expectedCount;
    }

    @Override
    public RowIsProcessedStrategy create() {
        return new SyncRowIsProcessedStrategy(expectedCount);
    }
}
