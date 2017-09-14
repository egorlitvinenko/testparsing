package org.egorlitvinenko.testdisruptor.clickhousestream;

import com.lmax.disruptor.EventHandler;
import org.egorlitvinenko.testdisruptor.clickhousestream.ch.ClickhouseInsertBatch;
import org.egorlitvinenko.testdisruptor.clickhousestream.event.LineEvent;

/**
 * @author Egor Litvinenko
 */
public class ChLineEventHandler implements EventHandler<LineEvent> {

    private final ClickhouseInsertBatch insertBatch;

    public ChLineEventHandler(ClickhouseInsertBatch insertBatch) {
        this.insertBatch = insertBatch;
    }

    @Override
    public void onEvent(LineEvent event, long sequence, boolean endOfBatch) throws Exception {
        if (event.endStream()) {
            insertBatch.endBatching();
        } else if (event.isFinished() && event.isValid()) {
            insertBatch.addBatch(event.values());
            if (insertBatch.readyToBatch()) {
                insertBatch.execute();
            }
        }
    }

}
