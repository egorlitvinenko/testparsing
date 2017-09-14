package org.egorlitvinenko.testdisruptor.clickhousestream.handler;

import com.lmax.disruptor.EventHandler;
import org.egorlitvinenko.testdisruptor.clickhousestream.event.LineEvent;
import org.egorlitvinenko.testdisruptor.clickhousestream.validators.DoubleCheckCharacters;

/**
 * @author Egor Litvinenko
 */
public class DoubleValidation implements EventHandler<LineEvent> {

    private final int[] myColumns;
    private final int id;

    public DoubleValidation(int id, int[] myColumns) {
        this.id = id;
        this.myColumns = myColumns;
    }

    @Override
    public void onEvent(LineEvent event, long sequence, boolean endOfBatch) throws Exception {
        for (int i = 0; i < myColumns.length; ++i) {
            String value = event.values()[myColumns[i]];
            if (!event.isValid()) {
                break;
            }
            if (!DoubleCheckCharacters.plainCharAt(value)) {
                event.markAsInvalid();
                break;
            }
        }
        event.markAsFinished(id);
    }
}
