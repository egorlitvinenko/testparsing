package org.egorlitvinenko.testdisruptor.clickhousestream;

import com.lmax.disruptor.dsl.Disruptor;
import org.egorlitvinenko.testdisruptor.Clickhouse;
import org.egorlitvinenko.testdisruptor.clickhousestream.ch.BufferPreparedStream;
import org.egorlitvinenko.testdisruptor.clickhousestream.ch.ClickhouseHttp;
import org.egorlitvinenko.testdisruptor.clickhousestream.ch.ClickhouseInsertBatch;
import org.egorlitvinenko.testdisruptor.clickhousestream.disruptor.LineEventDisruptor;
import org.egorlitvinenko.testdisruptor.clickhousestream.event.LineEvent;
import org.egorlitvinenko.testdisruptor.clickhousestream.reader.UnivosityReaderFromQuotedCsv;
import org.egorlitvinenko.testdisruptor.smallstream.util.TestDataProvider;
import org.egorlitvinenko.testdisruptor.smallstream.util.ThreadFactories;
import org.springframework.util.StopWatch;

import java.util.concurrent.ThreadFactory;

/**
 * @author Egor Litvinenko
 */
public class UnivosityDisruptorBaselineMain {

    public static final int BATCH_SIZE = 7000;

    public static void main(String[] args) throws Exception {
        System.out.println("Enter to start...");
        System.in.read();
        runTest();
    }

    public static void runTest() throws Exception {

        ClickhouseHttp clickhouseHttp = new ClickhouseHttp("jdbc:clickhouse://localhost:9123");
        ClickhouseInsertBatch insertBatch = new ClickhouseInsertBatch(clickhouseHttp,
                "INSERT INTO test.TEST_DATA_1M_9C_DATE (ID, f1, f2, f3, f4, f5, f6, f7, f8)",
                BATCH_SIZE,
//                new ClickhousePreparedStream(BATCH_SIZE * 1000, true)
                new BufferPreparedStream(BATCH_SIZE * 1000)
        );

        TestDataProvider.Data testData = TestDataProvider.R_1M__S_1__SQL_DATE_1__I_4__DOUBLE_4__E_0;

        Clickhouse.setApacheHttpClientLoggingSettings();

        ThreadFactory threadFactory = ThreadFactories.simpleDaemonFactory();

        LineEventDisruptor parseCsvDisruptor = new LineEventDisruptor();
        Disruptor<LineEvent> lineEventDisruptor =
                parseCsvDisruptor.create(threadFactory,
                        testData.columnTypes,
                        new ChLineEventHandler(insertBatch));

        UnivosityReaderFromQuotedCsv reader = new UnivosityReaderFromQuotedCsv(',', '"',
                testData.columnTypes,
                lineEventDisruptor.getRingBuffer());

        StopWatch common = new StopWatch();
        StopWatch stopWatch = new StopWatch();
        int warmup = 0;
        for (int i = -warmup; i <= 1000; ++i) {
            if (i == 0) {
                common.start("Write CSV to CH - All");
            } else if (i < 0) {
                reader.readFile(testData.file);
            } else {
                stopWatch.start("Write CSV to CH - " + i);
                reader.readFile(testData.file);
                stopWatch.stop();
            }
        }
        lineEventDisruptor.shutdown();
        common.stop();
        System.out.println(stopWatch.prettyPrint());
        System.out.println(common.prettyPrint());
    }

}
