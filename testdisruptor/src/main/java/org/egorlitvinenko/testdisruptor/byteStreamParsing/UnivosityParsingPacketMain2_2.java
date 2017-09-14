package org.egorlitvinenko.testdisruptor.byteStreamParsing;

import com.lmax.disruptor.dsl.Disruptor;
import org.egorlitvinenko.testdisruptor.Clickhouse;
import org.egorlitvinenko.testdisruptor.byteStreamParsing.adapter.SimpleTableRowAndPrepareStatementAdapter;
import org.egorlitvinenko.testdisruptor.byteStreamParsing.disruptor.ParsePacketDisruptor2;
import org.egorlitvinenko.testdisruptor.byteStreamParsing.event.ParsePacketEvent;
import org.egorlitvinenko.testdisruptor.byteStreamParsing.handler.parsePacket.ClickhouseParsePacketHandler;
import org.egorlitvinenko.testdisruptor.byteStreamParsing.reader.UnivosityReaderFromQuotedCsv3;
import org.egorlitvinenko.testdisruptor.byteStreamParsing.reader.UnivosityReaderToMemory;
import org.egorlitvinenko.testdisruptor.smallstream.util.TestDataProvider;
import org.egorlitvinenko.testdisruptor.smallstream.util.ThreadFactories;
import org.springframework.util.StopWatch;

import java.util.concurrent.ThreadFactory;

/**
 * @author Egor Litvinenko
 */
public class UnivosityParsingPacketMain2_2 {

    public static void main(String[] args) throws Exception {
        System.out.println("Enter to start...");
        System.in.read();
        runTestWithReadFile();
    }

    public static void runTestForParsing() throws Exception {

        Clickhouse.setApacheHttpClientLoggingSettings();

        ThreadFactory threadFactory = ThreadFactories.simpleDaemonFactory();

        final TestDataProvider.Data testData = TestDataProvider.R_1M__S_1__SQL_DATE_1__I_4__DOUBLE_4__E_0;

        ParsePacketDisruptor2 parseCsvDisruptor = new ParsePacketDisruptor2(testData.columnTypes);
        Disruptor<ParsePacketEvent> parsePacketTableRowEventDisruptor =
                parseCsvDisruptor.createWriteToClickhouse(threadFactory,
                        testData.columnTypes,
                        new ClickhouseParsePacketHandler(
                                testData.insert,
                                15000,
                                new SimpleTableRowAndPrepareStatementAdapter()
                        ));

        UnivosityReaderToMemory reader = new UnivosityReaderToMemory( 1_000_000,
                ',', '"',
                testData.columnTypes,
                parsePacketTableRowEventDisruptor.getRingBuffer());
        reader.readFile(testData.file);

        StopWatch common = new StopWatch();
        common.start("Write CSV to CH - All");
        for (int i = 0; i < 1; ++i) {
            reader.goOverLines();
        }
        parsePacketTableRowEventDisruptor.shutdown();
        common.stop();
        System.out.println(common.prettyPrint());
    }

    public static void runTestWithReadFile() throws Exception {

        Clickhouse.setApacheHttpClientLoggingSettings();

        ThreadFactory threadFactory = ThreadFactories.simpleDaemonFactory();

        final TestDataProvider.Data testData = TestDataProvider.R_1M__S_1__SQL_DATE_1__I_4__DOUBLE_4__E_0;

        ParsePacketDisruptor2 parseCsvDisruptor = new ParsePacketDisruptor2(testData.columnTypes);
        Disruptor<ParsePacketEvent> parsePacketTableRowEventDisruptor =
                parseCsvDisruptor.createWriteToClickhouse(threadFactory,
                        testData.columnTypes,
                        new ClickhouseParsePacketHandler(
                                testData.insert,
                                15000,
                                new SimpleTableRowAndPrepareStatementAdapter()
                        ));

        UnivosityReaderFromQuotedCsv3 reader = new UnivosityReaderFromQuotedCsv3( ',', '"',
                testData.columnTypes,
                parsePacketTableRowEventDisruptor.getRingBuffer());

        StopWatch common = new StopWatch();
        StopWatch stopWatch = new StopWatch();
        int warmup = 20;
        for (int i = -warmup; i <= 100; ++i) {
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
        parsePacketTableRowEventDisruptor.shutdown();
        common.stop();
        System.out.println(stopWatch.prettyPrint());
        System.out.println(common.prettyPrint());
    }

}
