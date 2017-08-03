package org.egorlitvinenko.testdisruptor.smallstream.handler;

import org.egorlitvinenko.testdisruptor.Clickhouse;
import org.egorlitvinenko.testdisruptor.smallstream.event.NewParsedBatchEvent;
import org.egorlitvinenko.testdisruptor.smallstream.util.Types;
import com.lmax.disruptor.EventHandler;

import java.sql.Connection;
import java.sql.Date;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.time.LocalDate;

/**
 * @author Egor Litvinenko
 */
public class ClickhouseTypedBatchSaver implements EventHandler<NewParsedBatchEvent>, AutoCloseable {

    private final PreparedStatement preparedStatement;
    private final Connection connection;

    public ClickhouseTypedBatchSaver(String insert) {
        try {
            this.connection = Clickhouse.it().getConnection();
            this.preparedStatement = connection.prepareStatement(insert);
            this.connection.setAutoCommit(false);
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public void onEvent(NewParsedBatchEvent event, long sequence, boolean endOfBatch) throws Exception {
        for (int i = 0; i < event.getSize(); ++i) {
            for (int j = 0; j < event.getTypes().length; ++j) {
                setValue(preparedStatement, j + 1, event.getTypes()[j], event.getBatch()[i][j]);
            }
            this.preparedStatement.addBatch();
        }
        this.preparedStatement.executeBatch();
    }

    private static void setValue(PreparedStatement ps, int index, byte type, Object value) throws Exception {
        switch (type) {
            case Types.DOUBLE:
                ps.setDouble(index, (Double) value);
                return;
            case Types.INT_32:
                ps.setInt(index, (Integer) value);
                break;
            case Types.LOCAL_DATE:
                ps.setDate(index, Date.valueOf((LocalDate) value));
                break;
            case Types.STRING:
            default:
                ps.setString(index, (String) value);
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
