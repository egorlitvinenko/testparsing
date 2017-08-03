package org.egorlitvinenko.testdisruptor.smallstream.handler;

import org.egorlitvinenko.testdisruptor.Clickhouse;
import org.egorlitvinenko.testdisruptor.smallstream.event.NewBatchEvent;
import com.lmax.disruptor.EventHandler;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;

/**
 * @author Egor Litvinenko
 */
public class ClickhouseStringBatchSaver implements EventHandler<NewBatchEvent>, AutoCloseable {

    public static final int BATCH_SIZE = 15000;

    private final PreparedStatement preparedStatement;
    private final Connection connection;

    public ClickhouseStringBatchSaver(String insert) {
        try {
            this.connection = Clickhouse.it().getConnection();
            this.preparedStatement = connection.prepareStatement(insert);
            this.connection.setAutoCommit(false);
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public void onEvent(NewBatchEvent event, long sequence, boolean endOfBatch) throws Exception {
        for (String[] line : event.getBatch()) {
            for (int i = 0; i < line.length; ++i) {
                this.preparedStatement.setString(i + 1, line[i]);
            }
            this.preparedStatement.addBatch();
        }
        this.preparedStatement.executeBatch();
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
