package org.egorlitvinenko.testdisruptor.byteStreamParsing.factory;

import org.egorlitvinenko.testdisruptor.byteStreamParsing.model.TableRow;
import org.egorlitvinenko.testdisruptor.byteStreamParsing.model.TableRowIndexModel;
import org.egorlitvinenko.testdisruptor.byteStreamParsing.model.TableRowTypeModel;
import org.egorlitvinenko.testdisruptor.byteStreamParsing.model.TableRowWithArrays;
import org.egorlitvinenko.testdisruptor.byteStreamParsing.util.ColumnType;

/**
 * @author Egor Litvinenko
 */
public class PooledTableRowFactory implements TableRowFactory {

    private final TableRowTypeModel typeModel;
    private final TableRowIndexModel indexModel;
    private final ArrayTableRowPool pool;

    public PooledTableRowFactory(ColumnType[] types,
                                 RowIsProcessedStrategyFactory rowIsProcessedStrategyFactory) {
        this.typeModel = new TableRowTypeModel(types);
        this.indexModel = new TableRowIndexModel(types);
        this.pool = ArrayTableRowPool.allocate(100_100, new ArrayTableRowPool.TableRowProvider() {
            @Override
            public TableRow create() {
                return new TableRowWithArrays(indexModel, typeModel);
            }
        });
    }

    @Override
    public TableRow create() {
        return this.pool.get();
    }
}
