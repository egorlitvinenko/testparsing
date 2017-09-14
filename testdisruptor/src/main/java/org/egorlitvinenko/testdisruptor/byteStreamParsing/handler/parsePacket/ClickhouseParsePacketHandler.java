package org.egorlitvinenko.testdisruptor.byteStreamParsing.handler.parsePacket;

import com.lmax.disruptor.EventHandler;
import org.egorlitvinenko.testdisruptor.Clickhouse;
import org.egorlitvinenko.testdisruptor.byteStreamParsing.adapter.PreparedStatementRowValuesAdapter;
import org.egorlitvinenko.testdisruptor.byteStreamParsing.adapter.RowValuesAdapter;
import org.egorlitvinenko.testdisruptor.byteStreamParsing.adapter.TableRowAndPrepareStatementAdapter;
import org.egorlitvinenko.testdisruptor.byteStreamParsing.event.ParsePacketEvent;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;

/**
 * @author Egor Litvinenko
 */
public class ClickhouseParsePacketHandler implements EventHandler<ParsePacketEvent>, AutoCloseable {

    private final RowValuesAdapter adapter;
    private final PreparedStatement preparedStatement;
    private final Connection connection;
    private final int batchSize;

    private int counter = 0;

    public ClickhouseParsePacketHandler(String insert,
                                        int batchSize,
                                        @Deprecated
                                        TableRowAndPrepareStatementAdapter adapter) {
        try {
            this.connection = Clickhouse.it().getConnection();
            this.preparedStatement = connection.prepareStatement(insert);
            this.connection.setAutoCommit(false);
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
        this.adapter = new PreparedStatementRowValuesAdapter(this.preparedStatement);
        this.batchSize = batchSize;
    }

    @Override
    public void onEvent(ParsePacketEvent event, long sequence, boolean endOfBatch) throws Exception {
        if (event.isEnd()) {
            if (this.counter > 0) {
                this.preparedStatement.executeBatch();
            }
        } else if (event.rowIsProcessed()) {
            event.visit(this.adapter);
            event.close();
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
