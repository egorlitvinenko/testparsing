package org.egorlitvinenko.testdisruptor.clickhousestream.event;

import org.egorlitvinenko.testdisruptor.clickhousestream.model.Line;
import org.egorlitvinenko.testdisruptor.clickhousestream.model.PaddedBoolean;

import java.util.concurrent.atomic.AtomicBoolean;

/**
 * @author Egor Litvinenko
 */
public class LineEvent implements Line {

    private boolean endStream;

    private String[] values;

    private AtomicBoolean invalid;
    private AtomicBoolean[] isFinished;

    public LineEvent(int threads) {
        invalid = new AtomicBoolean(false);
        isFinished = new AtomicBoolean[threads];
        for (int i = 0; i < isFinished.length; ++i) {
            isFinished[i] = new AtomicBoolean(false);
        }
    }

    public void setEndStream(boolean endStream) {
        this.endStream = endStream;
    }

    public void setValues(String[] values) {
        this.values = values;
    }

    @Override
    public boolean endStream() {
        return endStream;
    }

    @Override
    public String[] values() {
        return values;
    }

    @Override
    public void markAsInvalid() {
        invalid.set(true);
    }

    @Override
    public void markAsFinished(int index) {
        isFinished[index].set(true);
    }

    @Override
    public boolean isValid() {
        return !invalid.get();
    }

    @Override
    public boolean isFinished() {
        return invalid.get()
                ||
                isTrue(isFinished);
    }

    @Override
    public void reset() {
        invalid = new AtomicBoolean(false);
        for (int i = 0; i < isFinished.length; ++i) {
            isFinished[i].set(false);
        }
    }

    private static boolean isTrue(AtomicBoolean[] values) {
        for (int i = 0; i < values.length; ++i) {
            if (!values[i].get())
                return false;
        }
        return true;
    }
}
