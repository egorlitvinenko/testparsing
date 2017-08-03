package org.egorlitvinenko.testdisruptor.byteStreamParsing.handler;

import org.egorlitvinenko.testdisruptor.Clickhouse;
import org.egorlitvinenko.testdisruptor.byteStreamParsing.adapter.TableRowAndPrepareStatementAdapter;
import org.egorlitvinenko.testdisruptor.byteStreamParsing.event.ParsedTableRowEvent;
import com.lmax.disruptor.EventHandler;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;

/**
 * @author Egor Litvinenko
 */
public class ClichouseParsedTableRowHandler implements EventHandler<ParsedTableRowEvent>, AutoCloseable {

    public static int BATCH_SIZE = 15000;

    private final TableRowAndPrepareStatementAdapter adapter;
    private final PreparedStatement preparedStatement;
    private final Connection connection;

    private int counter = 0;

    public ClichouseParsedTableRowHandler(String insert, TableRowAndPrepareStatementAdapter tableRowAndPrepareStatementAdapter) {
        try {
            this.connection = Clickhouse.it().getConnection();
            this.preparedStatement = connection.prepareStatement(insert);
            this.connection.setAutoCommit(false);
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
        this.adapter = tableRowAndPrepareStatementAdapter;
    }

    @Override
    public void onEvent(ParsedTableRowEvent event, long sequence, boolean endOfBatch) throws Exception {
        if (event.getTableRow().rowIsProcessed()) {
            this.adapter.adopt(event.getTableRow(), preparedStatement);
            event.getTableRow().close();
            this.preparedStatement.addBatch();
            this.counter++;
            if (this.counter >= BATCH_SIZE) {
                this.preparedStatement.executeBatch();
                this.counter = 0;
            }
        }
    }

    @Override
    public void close() throws Exception {
        try {
            preparedStatement.close();
        } finally {
            connection.close();
        }
    }

}
