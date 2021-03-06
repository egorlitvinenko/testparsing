package org.egorlitvinenko.testdisruptor.byteStreamParsing;

import com.lmax.disruptor.dsl.Disruptor;
import org.egorlitvinenko.testdisruptor.Clickhouse;
import org.egorlitvinenko.testdisruptor.byteStreamParsing.adapter.SimpleTableRowAndPrepareStatementAdapter;
import org.egorlitvinenko.testdisruptor.byteStreamParsing.disruptor.UnsafeParsePacketDisruptor;
import org.egorlitvinenko.testdisruptor.byteStreamParsing.event.UnsafeParsePacketEvent;
import org.egorlitvinenko.testdisruptor.byteStreamParsing.handler.unsafeParsePacket.ClickhouseUnsafeParsePacketHandler;
import org.egorlitvinenko.testdisruptor.byteStreamParsing.reader.UnsafeUnivosityReaderFromQuotedCsv;
import org.egorlitvinenko.testdisruptor.smallstream.util.TestDataProvider;
import org.egorlitvinenko.testdisruptor.smallstream.util.ThreadFactories;
import org.springframework.util.StopWatch;

import java.util.concurrent.ThreadFactory;

/**
 * @author Egor Litvinenko
 */
public class UnivosityParsingPacketUnsafeMain {

    public static void main(String[] args) throws Exception {
        System.out.println("Enter to start...");
        System.in.read();
        runTest();
    }

    public static void runTest() throws Exception {

        Clickhouse.setApacheHttpClientLoggingSettings();

        ThreadFactory threadFactory = ThreadFactories.simpleDaemonFactory();

        final TestDataProvider.Data testData = TestDataProvider.R_1M__S_1__SQL_DATE_1__I_4__DOUBLE_4__E_0;

        UnsafeParsePacketDisruptor parseCsvDisruptor = new UnsafeParsePacketDisruptor(testData.columnTypes);
        Disruptor<UnsafeParsePacketEvent> parsePacketTableRowEventDisruptor =
                parseCsvDisruptor.createWriteToClickhouse(threadFactory,
                        testData.columnTypes,
                        new ClickhouseUnsafeParsePacketHandler(
                                testData.insert,
                                15000,
                                new SimpleTableRowAndPrepareStatementAdapter()
                        ));

        UnsafeUnivosityReaderFromQuotedCsv reader = new UnsafeUnivosityReaderFromQuotedCsv( ',', '"',
                testData.columnTypes,
                parsePacketTableRowEventDisruptor.getRingBuffer());

        StopWatch common = new StopWatch();
        common.start("Write CSV to CH - All");
        StopWatch stopWatch = new StopWatch();
        for (int i = 0; i < 10; ++i) {
            stopWatch.start("Write CSV to CH - " + i);
            reader.readFile(testData.file);
            stopWatch.stop();
        }
        parsePacketTableRowEventDisruptor.shutdown();
        common.stop();
        System.out.println(stopWatch.prettyPrint());
        System.out.println(common.prettyPrint());

    }

}