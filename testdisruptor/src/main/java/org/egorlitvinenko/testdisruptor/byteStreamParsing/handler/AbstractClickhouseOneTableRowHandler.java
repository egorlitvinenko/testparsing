package org.egorlitvinenko.testdisruptor.byteStreamParsing.handler;

import com.lmax.disruptor.EventHandler;
import org.egorlitvinenko.testdisruptor.Clickhouse;
import org.egorlitvinenko.testdisruptor.byteStreamParsing.adapter.TableRowAndPrepareStatementAdapter;
import org.egorlitvinenko.testdisruptor.byteStreamParsing.event.HasOneTableRowEvent;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;

/**
 * @author Egor Litvinenko
 */
public class AbstractClickhouseOneTableRowHandler<T extends HasOneTableRowEvent> implements EventHandler<T>, AutoCloseable {

    private final TableRowAndPrepareStatementAdapter adapter;
    private final PreparedStatement preparedStatement;
    private final Connection connection;
    private final int batchSize;

    private int counter = 0;

    public AbstractClickhouseOneTableRowHandler(String insert,
                                                int batchSize,
                                                TableRowAndPrepareStatementAdapter tableRowAndPrepareStatementAdapter) {
        try {
            this.connection = Clickhouse.it().getConnection();
            this.preparedStatement = connection.prepareStatement(insert);
            this.connection.setAutoCommit(false);
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
        this.adapter = tableRowAndPrepareStatementAdapter;
        this.batchSize = batchSize;
    }

    @Override
    public void onEvent(T event, long sequence, boolean endOfBatch) throws Exception {
        if (event.isEnd()) {
            if (this.counter > 0) {
                this.preparedStatement.executeBatch();
            }
        } else if (event.getTableRow().rowIsProcessed()) {
            this.adapter.adopt(event.getTableRow(), preparedStatement);
            event.getTableRow().close();
            this.preparedStatement.addBatch();
            this.counter++;
            if (this.counter >= batchSize) {
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
