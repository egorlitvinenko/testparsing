package org.egorlitvinenko.testdisruptor.byteStreamParsing.factory;

import org.egorlitvinenko.testdisruptor.byteStreamParsing.model.RowIsProcessedStrategy;

/**
 * @author Egor Litvinenko
 */
public interface RowIsProcessedStrategyFactory {

    RowIsProcessedStrategy create();

}
