package org.egorlitvinenko.testdisruptor.byteStreamParsing;

import org.egorlitvinenko.testdisruptor.Clickhouse;
import org.egorlitvinenko.testdisruptor.byteStreamParsing.adapter.SimpleTableRowAndPrepareStatementAdapter;
import org.egorlitvinenko.testdisruptor.byteStreamParsing.disruptor.ParseBatchCsvDisruptor;
import org.egorlitvinenko.testdisruptor.byteStreamParsing.event.ParseBatchTableRowEvent;
import org.egorlitvinenko.testdisruptor.byteStreamParsing.factory.SimpleTableRowFactory;
import org.egorlitvinenko.testdisruptor.byteStreamParsing.handler.batchAndCompareWithType.ClickhouseParseBatchTableRowHandler;
import org.egorlitvinenko.testdisruptor.byteStreamParsing.reader.UnivosityBatchReaderFromQuotedCsv;
import org.egorlitvinenko.testdisruptor.smallstream.util.TestDataProvider;
import org.egorlitvinenko.testdisruptor.smallstream.util.ThreadFactories;
import com.lmax.disruptor.dsl.Disruptor;
import org.springframework.util.StopWatch;

import java.util.concurrent.ThreadFactory;

/**
 * @author Egor Litvinenko
 */
public class UnivosityParsingBatchMain {

    public static final int TABLE_ROW_BATCH_SIZE = 10;

    public static void main(String[] args) throws Exception {
        System.out.println("Enter to start...");
        System.in.read();
        runTest();
    }

    public static void runTest() throws Exception {

        Clickhouse.setApacheHttpClientLoggingSettings();

        ThreadFactory threadFactory = ThreadFactories.simpleDaemonFactory();

        final TestDataProvider.Data testData = TestDataProvider.R_1M__S_1__DATE_1__I_4__DOUBLE_4__E_0;

        ParseBatchCsvDisruptor parseCsvDisruptor = new ParseBatchCsvDisruptor();
        Disruptor<ParseBatchTableRowEvent> parseBatchTableRowEventDisruptor =
                parseCsvDisruptor.createWriteToClickhouse(threadFactory,
                        testData.columnTypes,
                        new ClickhouseParseBatchTableRowHandler(
                                testData.insert,
                                new SimpleTableRowAndPrepareStatementAdapter()
                        ));

        UnivosityBatchReaderFromQuotedCsv reader = new UnivosityBatchReaderFromQuotedCsv(TABLE_ROW_BATCH_SIZE,
                ',', '"',
                testData.columnTypes,
                parseBatchTableRowEventDisruptor.getRingBuffer());

        SimpleTableRowFactory tableRowFactory = new SimpleTableRowFactory(
                testData.columnTypes
        );

        StopWatch common = new StopWatch();
        common.start("Write CSV to CH - All");
        StopWatch stopWatch = new StopWatch();
        for (int i = 0; i < 1; ++i) {
            stopWatch.start("Write CSV to CH - " + i);
            reader.readFile(testData.file, tableRowFactory);
            stopWatch.stop();
        }
        parseBatchTableRowEventDisruptor.shutdown();
        common.stop();
        System.out.println(stopWatch.prettyPrint());
        System.out.println(common.prettyPrint());

        // ~ 6.5 sec, TABLE_ROW_BATCH_SIZE < 100
        // 100 tests - ~0.005 per row in average, or 200000 rows per second.


    }

}
