package org.egorlitvinenko.testdisruptor.smallstream;

import org.egorlitvinenko.testdisruptor.smallstream.disruptor.NewBatchEventDisruptor;
import org.egorlitvinenko.testdisruptor.smallstream.disruptor.NewParsedBatchEventDisruptor;
import org.egorlitvinenko.testdisruptor.smallstream.event.NewBatchEvent;
import org.egorlitvinenko.testdisruptor.smallstream.event.NewParsedBatchEvent;
import org.egorlitvinenko.testdisruptor.smallstream.util.OpenCsvUtils;
import org.egorlitvinenko.testdisruptor.smallstream.util.TestDataProvider;
import org.egorlitvinenko.testdisruptor.smallstream.util.ThreadFactories;
import com.lmax.disruptor.dsl.Disruptor;
import org.springframework.util.StopWatch;

import java.util.concurrent.ThreadFactory;

/**
 * @author Egor Litvinenko
 */
public class OpenCsv5ParsedStringsToClickhouseMain {

    public static final int BATCH_SIZE = 15000;

    public static void main(String[] args) throws Exception {
        System.in.read();
        runTest();
    }

    public static void runTest() throws Exception {

        ThreadFactory threadFactory = ThreadFactories.simpleDaemonFactory();
        Disruptor<NewParsedBatchEvent> newParsedBatchEventDisruptor = new NewParsedBatchEventDisruptor()
                .createForClickhouseTypedBatchSaver(threadFactory, TestDataProvider.R_1M__S_1__DATE_1__I_4__DOUBLE_4__E_0.insert);
        Disruptor<NewBatchEvent> newBatchEventDisruptor = new NewBatchEventDisruptor().createForParsing(threadFactory,
                TestDataProvider.R_1M__S_1__DATE_1__I_4__DOUBLE_4__E_0.types,
                newParsedBatchEventDisruptor.getRingBuffer());

        StopWatch stopWatch = new StopWatch();
        stopWatch.start("Parse and Write to Clickhouse");
        OpenCsvUtils.readCsvFileWithBatches(TestDataProvider.R_1M__S_1__DATE_1__I_4__DOUBLE_4__E_0.file,
                newBatchEventDisruptor,
                BATCH_SIZE, true);
        newBatchEventDisruptor.shutdown();
        newParsedBatchEventDisruptor.shutdown();
        stopWatch.stop();
        System.out.println(stopWatch.prettyPrint());

        //___________________________
        // 1m rows, 1 string, 4 double
        // ~ 6 sec - 2000 batch size
        // ~ 6 sec - 5000 batch size
        // ~ 5 sec - 15000 batch size
        // ~ 5 sec - 50000 batch size
        //___________________________
        //___________________________
        // 1m rows, 1 string, 4 double, 4 integer
        // 10sec - 15000 bs
        //___________________________
        //___________________________
        // 1m rows, 1 string, 1 date, 4 double, 4 integer
        // 10sec - 15000 bs
        //___________________________
    }


}
