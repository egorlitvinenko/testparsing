package org.egorlitvinenko.testdisruptor.byteStreamParsing.adapter;

import org.egorlitvinenko.testdisruptor.byteStreamParsing.model.TableRow;

import java.sql.PreparedStatement;

/**
 * @author Egor Litvinenko
 */
public interface TableRowAndPrepareStatementAdapter {

    void adopt(TableRow tableRow, PreparedStatement preparedStatement) throws Exception;

}
