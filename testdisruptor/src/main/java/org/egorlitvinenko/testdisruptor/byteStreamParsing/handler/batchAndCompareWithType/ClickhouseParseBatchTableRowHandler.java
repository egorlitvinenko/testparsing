package org.egorlitvinenko.testdisruptor.byteStreamParsing.handler.batchAndCompareWithType;

import org.egorlitvinenko.testdisruptor.Clickhouse;
import org.egorlitvinenko.testdisruptor.byteStreamParsing.adapter.TableRowAndPrepareStatementAdapter;
import org.egorlitvinenko.testdisruptor.byteStreamParsing.event.ParseBatchTableRowEvent;
import com.lmax.disruptor.EventHandler;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;

/**
 * @author Egor Litvinenko
 */
public class ClickhouseParseBatchTableRowHandler implements EventHandler<ParseBatchTableRowEvent>, AutoCloseable {

    public static int BATCH_SIZE = 15000;

    private final TableRowAndPrepareStatementAdapter adapter;
    private final PreparedStatement preparedStatement;
    private final Connection connection;

    private int counter = 0;

    public ClickhouseParseBatchTableRowHandler(String insert, TableRowAndPrepareStatementAdapter tableRowAndPrepareStatementAdapter) {
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
    public void onEvent(ParseBatchTableRowEvent event, long sequence, boolean endOfBatch) throws Exception {
        if (event.isEnd()) {
            if (this.counter > 0) {
                this.preparedStatement.executeBatch();
            }
        } else {
            for (int j = 0; j < event.getSize(); j++) {
                if (event.getTableRows()[j].rowIsProcessed()
                        && !event.getTableRows()[j].isClosed()) {
                    this.adapter.adopt(event.getTableRows()[j], preparedStatement);
                    event.getTableRows()[j].close();
                    this.preparedStatement.addBatch();
                    this.counter++;
                    if (this.counter >= BATCH_SIZE) {
                        this.preparedStatement.executeBatch();
                        this.counter = 0;
                    }
                }
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
