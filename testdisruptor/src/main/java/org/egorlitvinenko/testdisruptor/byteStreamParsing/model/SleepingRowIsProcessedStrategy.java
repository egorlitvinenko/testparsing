package org.egorlitvinenko.testdisruptor.byteStreamParsing.model;

import org.apache.http.util.Asserts;

import java.util.Objects;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.LockSupport;

/**
 * @author Egor Litvinenko
 */
public class SleepingRowIsProcessedStrategy implements RowIsProcessedStrategy {

    private final Thread wakeUpThread;
    private final int repeat, expectedCount;

    private AtomicInteger counter;

    public SleepingRowIsProcessedStrategy(Thread wakeUpThread, int expectedCount) {
        this(200, wakeUpThread, expectedCount);
    }

    public SleepingRowIsProcessedStrategy(int repeat, Thread wakeUpThread, int expectedCount) {
        Objects.requireNonNull(wakeUpThread);
        Asserts.check(repeat > 0, "Repeat > 0");
        this.wakeUpThread = wakeUpThread;
        this.repeat = repeat;
        this.expectedCount = expectedCount;
        this.counter = new AtomicInteger(0);
    }

    @Override
    public int expectedCount() {
        return expectedCount;
    }

    @Override
    public void incrementProcessedElements() {
        counter.incrementAndGet();
        LockSupport.unpark(wakeUpThread);
    }

    @Override
    public boolean isFinished() {
        int counter = repeat;
        while (100 < counter--) {
            if (this.counter.get() == expectedCount) {
                return true;
            }
        }
        while (0 < counter--) {
            Thread.yield();
            if (this.counter.get() == expectedCount) {
                return true;
            }
        }
        LockSupport.parkNanos(1l);
        return false;
    }

    @Override
    public void reset() {
        LockSupport.unpark(wakeUpThread);
        this.counter.set(0);
    }

}
