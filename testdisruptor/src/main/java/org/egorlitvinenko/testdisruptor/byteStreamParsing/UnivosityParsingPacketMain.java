package org.egorlitvinenko.testdisruptor.byteStreamParsing;

import org.egorlitvinenko.testdisruptor.Clickhouse;
import org.egorlitvinenko.testdisruptor.byteStreamParsing.adapter.SimpleTableRowAndPrepareStatementAdapter;
import org.egorlitvinenko.testdisruptor.byteStreamParsing.disruptor.ParsePacketCsvDisruptor;
import org.egorlitvinenko.testdisruptor.byteStreamParsing.event.ParsePacketTableRowEvent;
import org.egorlitvinenko.testdisruptor.byteStreamParsing.factory.SimpleTableRowFactory;
import org.egorlitvinenko.testdisruptor.byteStreamParsing.handler.compareWithType.ClickhouseParsePacketTableRowHandler;
import org.egorlitvinenko.testdisruptor.byteStreamParsing.reader.UnivosityReaderFromQuotedCsv2;
import org.egorlitvinenko.testdisruptor.smallstream.util.TestDataProvider;
import org.egorlitvinenko.testdisruptor.smallstream.util.ThreadFactories;
import com.lmax.disruptor.dsl.Disruptor;
import org.springframework.util.StopWatch;

import java.util.concurrent.ThreadFactory;

/**
 * @author Egor Litvinenko
 */
public class UnivosityParsingPacketMain {

    public static void main(String[] args) throws Exception {
        System.out.println("Enter to start...");
        System.in.read();
        runTest();
    }

    public static void runTest() throws Exception {

        Clickhouse.setApacheHttpClientLoggingSettings();

        ThreadFactory threadFactory = ThreadFactories.simpleDaemonFactory();

        final TestDataProvider.Data testData = TestDataProvider.R_1M__S_1__SQL_DATE_1__I_4__DOUBLE_4__E_0;

        ParsePacketCsvDisruptor parseCsvDisruptor = new ParsePacketCsvDisruptor();
        Disruptor<ParsePacketTableRowEvent> parsePacketTableRowEventDisruptor =
                parseCsvDisruptor.createWriteToClickhouse(threadFactory,
                        testData.columnTypes,
                        new ClickhouseParsePacketTableRowHandler(
                                testData.insert,
                                new SimpleTableRowAndPrepareStatementAdapter()
                        ));

        UnivosityReaderFromQuotedCsv2 reader = new UnivosityReaderFromQuotedCsv2( ',', '"',
                testData.columnTypes,
                parsePacketTableRowEventDisruptor.getRingBuffer());

        SimpleTableRowFactory tableRowFactory = new SimpleTableRowFactory(
                testData.columnTypes
        );

        StopWatch stopWatch = new StopWatch();
        stopWatch.start("Write CSV to CH");
        reader.readFile(testData.file, tableRowFactory);
        parsePacketTableRowEventDisruptor.shutdown();
        stopWatch.stop();
        System.out.println(stopWatch.prettyPrint());


    }

}
