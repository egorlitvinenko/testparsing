package org.egorlitvinenko.testdisruptor.byteStreamParsing.factory;

import org.egorlitvinenko.testdisruptor.byteStreamParsing.util.ColumnType;
import org.egorlitvinenko.testdisruptor.byteStreamParsing.model.TableRow;
import org.egorlitvinenko.testdisruptor.byteStreamParsing.model.TableRowIndexModel;
import org.egorlitvinenko.testdisruptor.byteStreamParsing.model.TableRowTypeModel;
import org.egorlitvinenko.testdisruptor.byteStreamParsing.model.TableRowWithArrays;

/**
 * @author Egor Litvinenko
 */
public class SimpleTableRowFactory implements TableRowFactory {

    private final TableRowTypeModel typeModel;
    private final TableRowIndexModel indexModel;


    public SimpleTableRowFactory(ColumnType[] types) {
        this.typeModel = new TableRowTypeModel(types);
        this.indexModel = new TableRowIndexModel(types);
    }

    @Override
    public TableRow create() {
        return new TableRowWithArrays(indexModel, typeModel);
    }
}
