package org.egorlitvinenko.testdisruptor.smallstream.handler;

import org.egorlitvinenko.testdisruptor.Clickhouse;
import org.egorlitvinenko.testdisruptor.smallstream.event.NewCombinedStringLineEvent;
import com.lmax.disruptor.EventHandler;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;

/**
 * @author Egor Litvinenko
 */
public class ClickhouseStringLineSaver implements EventHandler<NewCombinedStringLineEvent>, AutoCloseable {

    private final PreparedStatement preparedStatement;
    private final Connection connection;

    private int counter = 0;

    public ClickhouseStringLineSaver(String insert) {
        try {
            this.connection = Clickhouse.it().getConnection();
            this.preparedStatement = connection.prepareStatement(insert);
            this.connection.setAutoCommit(false);
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public void onEvent(NewCombinedStringLineEvent event, long sequence, boolean endOfBatch) throws Exception {
        if (null != event.getLine()) {
            for (int i = 0; i < event.getLine().length; ++i) {
                this.preparedStatement.setString(i + 1, event.getLine()[i]);
            }
            this.preparedStatement.addBatch();
            this.counter++;
            if (counter >= ClickhouseStringBatchSaver.BATCH_SIZE) {
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
