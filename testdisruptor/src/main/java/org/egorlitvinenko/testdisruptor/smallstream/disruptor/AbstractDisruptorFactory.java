package org.egorlitvinenko.testdisruptor.smallstream.disruptor;

import com.lmax.disruptor.EventFactory;
import com.lmax.disruptor.EventHandler;
import com.lmax.disruptor.RingBuffer;
import com.lmax.disruptor.WaitStrategy;
import com.lmax.disruptor.dsl.Disruptor;
import com.lmax.disruptor.dsl.ProducerType;

import java.util.concurrent.ThreadFactory;

/**
 * @author Egor Litvinenko
 */
public abstract class AbstractDisruptorFactory<Event> {

    public Disruptor<Event> create(ThreadFactory threadFactory,
                                   EventHandler<Event>... handlers) {
        // The factory for the event
        EventFactory<Event> factory = createEventFactory();

        // Specify the size of the ring buffer, must be power of 2.
        int bufferSize = getRingBufferSize();

        // Construct the Disruptor
        Disruptor<Event> disruptor = new Disruptor<>(factory, bufferSize, threadFactory,
                getProducerType(), getWaitStrategy());

        // Connect the handler
        disruptor.handleEventsWith(handlers);

        // Start the Disruptor, starts all threads running
        RingBuffer<Event> ringBuffer = disruptor.start();

        return disruptor;
    }

    protected abstract EventFactory<Event> createEventFactory();

    protected abstract WaitStrategy getWaitStrategy();

    protected abstract ProducerType getProducerType();

    protected abstract int getRingBufferSize();

}
