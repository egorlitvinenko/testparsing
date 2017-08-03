package org.egorlitvinenko.testdisruptor.smallstream.event;

import org.egorlitvinenko.testdisruptor.smallstream.util.ArrayUtils;
import com.lmax.disruptor.EventTranslatorTwoArg;

/**
 * @author Egor Litvinenko
 */
public class NewBatchEvent {

    private String[][] batch;
    private int size;


    public int getSize() {
        return size;
    }

    public void setSize(int size) {
        this.size = size;
    }

    public String[][] getBatch() {
        return batch;
    }

    public void setBatch(String[][] batch) {
        this.batch = batch;
    }

    public static EventTranslatorTwoArg<NewBatchEvent, String[][], Integer> TRANSLATOR = new EventTranslatorTwoArg<NewBatchEvent, String[][], Integer>() {
        @Override
        public void translateTo(NewBatchEvent event, long sequence, String[][] arg0, Integer arg1) {
            event.setSize(arg1);
            event.setBatch(arg0);
        }
    };

    public static EventTranslatorTwoArg<NewBatchEvent, String[][], Integer> COPY_TRANSLATOR = new EventTranslatorTwoArg<NewBatchEvent, String[][], Integer>() {
        @Override
        public void translateTo(NewBatchEvent event, long sequence, String[][] arg0, Integer arg1) {
            event.setSize(arg1);
            event.setBatch(ArrayUtils.copy(arg0, arg1));
        }
    };

}
