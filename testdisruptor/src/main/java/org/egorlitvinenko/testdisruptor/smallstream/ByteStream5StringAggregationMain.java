package org.egorlitvinenko.testdisruptor.smallstream;

import org.egorlitvinenko.testdisruptor.smallstream.disruptor.NewByteEventDisruptor;
import org.egorlitvinenko.testdisruptor.smallstream.disruptor.NewCombinedStringLineDisruptor;
import org.egorlitvinenko.testdisruptor.smallstream.disruptor.NewStringEventDisruptor;
import org.egorlitvinenko.testdisruptor.smallstream.event.NewByteEvent;
import org.egorlitvinenko.testdisruptor.smallstream.event.NewCombinedStringLineEvent;
import org.egorlitvinenko.testdisruptor.smallstream.event.NewStringEvent;
import org.egorlitvinenko.testdisruptor.smallstream.factory.NewCharacterEventProducer;
import org.egorlitvinenko.testdisruptor.smallstream.util.TestDataProvider;
import org.egorlitvinenko.testdisruptor.smallstream.util.ThreadFactories;
import com.lmax.disruptor.RingBuffer;
import com.lmax.disruptor.dsl.Disruptor;
import org.springframework.util.StopWatch;

import java.nio.ByteBuffer;
import java.nio.channels.SeekableByteChannel;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.concurrent.ThreadFactory;

/**
 * @author Egor Litvinenko
 */
public class ByteStream5StringAggregationMain {

    public static int FILE_BUFFER = 8192;
    public static int NEW_BYTE_BUFFER = 1024 * 64;
    public static int NEW_STRING_BUFFER = 1024 * 64;
    public static int NEW_LINE_BUFFER = 1024 * 64;

    public static void main(String[] args) throws Exception {

        ThreadFactory threadFactory = ThreadFactories.simpleDaemonFactory();
        Disruptor<NewCombinedStringLineEvent> newCombinedStringLineEventResult = new NewCombinedStringLineDisruptor().createForClickhouseLineSaver(threadFactory);
        Disruptor<NewStringEvent> newStringEventResult = new NewStringEventDisruptor().createForLineAggregator(5, threadFactory, newCombinedStringLineEventResult.getRingBuffer());
        Disruptor<NewByteEvent> newCharacterEventResult = new NewByteEventDisruptor().createForStringAggregator(threadFactory, newStringEventResult.getRingBuffer());

        StopWatch stopWatch = new StopWatch();
        stopWatch.start("Write to Clickhouse");
        readCsvAndWrite(newCharacterEventResult.getRingBuffer());
        stopWatch.stop();
        System.out.println(stopWatch.prettyPrint());

        newCombinedStringLineEventResult.shutdown();
        newStringEventResult.shutdown();
        newCharacterEventResult.shutdown();

    }

    public static void readCsvAndWrite(RingBuffer<NewByteEvent> ringBuffer) throws Exception {
        SeekableByteChannel fileChannel = Files.newByteChannel(Paths.get(TestDataProvider.ROW_1M__STRING_1__DOUBLE_4__ERROR_1));
        int bufferSize = FILE_BUFFER;
        ByteBuffer byteBuffer = ByteBuffer.allocate(bufferSize);
        int read = 0, processed = 0;
        while ((read = fileChannel.read(byteBuffer)) > 0) {
            byteBuffer.rewind();
            byteBuffer.limit(read);
            for (int i = 0; i < read; ++i) {
                NewCharacterEventProducer.onData(ringBuffer, byteBuffer.get());
                processed++;
            }
            byteBuffer.flip();
        }
        System.out.println("Read is " + read);
        System.out.println("Processed is " + processed);
    }

}
