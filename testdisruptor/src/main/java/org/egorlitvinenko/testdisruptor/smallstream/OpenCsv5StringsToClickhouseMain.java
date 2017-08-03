package org.egorlitvinenko.testdisruptor.smallstream;

import org.egorlitvinenko.testdisruptor.smallstream.disruptor.NewBatchEventDisruptor;
import org.egorlitvinenko.testdisruptor.smallstream.event.NewBatchEvent;
import org.egorlitvinenko.testdisruptor.smallstream.handler.ClickhouseStringBatchSaver;
import org.egorlitvinenko.testdisruptor.smallstream.util.OpenCsvUtils;
import org.egorlitvinenko.testdisruptor.smallstream.util.TestDataProvider;
import org.egorlitvinenko.testdisruptor.smallstream.util.ThreadFactories;
import com.lmax.disruptor.dsl.Disruptor;
import org.springframework.util.StopWatch;

import java.util.concurrent.ThreadFactory;

/**
 * @author Egor Litvinenko
 */
public class OpenCsv5StringsToClickhouseMain {

    public static void main(String[] args) throws Exception {
        System.in.read();
        runTest();
    }

    public static void runTest() throws Exception {

        ThreadFactory threadFactory = ThreadFactories.simpleDaemonFactory();
        Disruptor<NewBatchEvent> disruptor = new NewBatchEventDisruptor().createForClickhouseBatchSaver(threadFactory);
        StopWatch stopWatch = new StopWatch();
        stopWatch.start("Write to Clickhouse");
        OpenCsvUtils.readCsvFileWithBatches(TestDataProvider.ROW_1M__STRING_1__DOUBLE_4__ERROR_1, disruptor, ClickhouseStringBatchSaver.BATCH_SIZE, true);
        stopWatch.stop();
        System.out.println(stopWatch.prettyPrint());
        disruptor.shutdown();
        // ~ 3.5 sec - batch size 15000, with copyBatches = true
    }


}
