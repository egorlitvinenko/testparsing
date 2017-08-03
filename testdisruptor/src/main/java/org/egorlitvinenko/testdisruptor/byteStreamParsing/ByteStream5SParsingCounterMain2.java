package org.egorlitvinenko.testdisruptor.byteStreamParsing;

import org.egorlitvinenko.testdisruptor.Clickhouse;
import org.egorlitvinenko.testdisruptor.byteStreamParsing.disruptor.ParseCsvDisruptor;
import org.egorlitvinenko.testdisruptor.byteStreamParsing.event.ParseTableRowEvent;
import org.egorlitvinenko.testdisruptor.byteStreamParsing.factory.SimpleTableRowFactory;
import org.egorlitvinenko.testdisruptor.byteStreamParsing.reader.ByteStreamReaderFromQuotedCsv2;
import org.egorlitvinenko.testdisruptor.smallstream.util.TestDataProvider;
import org.egorlitvinenko.testdisruptor.smallstream.util.ThreadFactories;
import com.lmax.disruptor.dsl.Disruptor;
import org.springframework.util.StopWatch;

import java.util.concurrent.ThreadFactory;

/**
 * @author Egor Litvinenko
 */
public class ByteStream5SParsingCounterMain2 {

    public static void main(String[] args) throws Exception {
        System.out.println("Enter to start...");
        System.in.read();
        runTest();
    }

    public static void runTest() throws Exception {

        Clickhouse.setApacheHttpClientLoggingSettings();

        ThreadFactory threadFactory = ThreadFactories.simpleDaemonFactory();

        ParseCsvDisruptor parseCsvDisruptor = new ParseCsvDisruptor();
        Disruptor<ParseTableRowEvent> parseTableRowEventDisruptor =
                parseCsvDisruptor.createCounter(threadFactory, TestDataProvider.R_1M__S_1__DATE_1__I_4__DOUBLE_4__E_0.columnTypes);

        ByteStreamReaderFromQuotedCsv2 reader = new ByteStreamReaderFromQuotedCsv2((byte) '\n', (byte) ',', (byte) '"',
                TestDataProvider.R_1M__S_1__DATE_1__I_4__DOUBLE_4__E_0.columnTypes,
                parseTableRowEventDisruptor.getRingBuffer());

        SimpleTableRowFactory tableRowFactory = new SimpleTableRowFactory(
                TestDataProvider.R_1M__S_1__DATE_1__I_4__DOUBLE_4__E_0.columnTypes
        );

        StopWatch stopWatch = new StopWatch();
        stopWatch.start("Parsing rows");
        reader.readFile(TestDataProvider.R_1M__S_1__DATE_1__I_4__DOUBLE_4__E_0.file,
                tableRowFactory);
        parseTableRowEventDisruptor.shutdown();
        stopWatch.stop();
        System.out.println(stopWatch.prettyPrint());
        System.out.println(parseCsvDisruptor.getCounter());
        System.out.println(parseCsvDisruptor.getCalls());

    }

}
