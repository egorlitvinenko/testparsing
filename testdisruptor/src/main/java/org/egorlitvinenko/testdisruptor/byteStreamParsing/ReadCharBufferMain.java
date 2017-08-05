package org.egorlitvinenko.testdisruptor.byteStreamParsing;

import com.lmax.disruptor.EventTranslatorOneArg;
import com.lmax.disruptor.dsl.Disruptor;
import org.egorlitvinenko.testdisruptor.Clickhouse;
import org.egorlitvinenko.testdisruptor.byteStreamParsing.adapter.SimpleTableRowAndPrepareStatementAdapter;
import org.egorlitvinenko.testdisruptor.byteStreamParsing.disruptor.ParseCharBufferCsvDisruptor;
import org.egorlitvinenko.testdisruptor.byteStreamParsing.disruptor.ReadCharBufferDisruptor;
import org.egorlitvinenko.testdisruptor.byteStreamParsing.event.ParseCharBufferTableRowEvent;
import org.egorlitvinenko.testdisruptor.byteStreamParsing.event.ReadFileToCharBufferEvent;
import org.egorlitvinenko.testdisruptor.byteStreamParsing.factory.SimpleTableRowFactory;
import org.egorlitvinenko.testdisruptor.byteStreamParsing.handler.batchAndCompareWithType.ClickhouseParseCharBufferTableRowHandler;
import org.egorlitvinenko.testdisruptor.byteStreamParsing.handler.charBufferAndcompareWithType.ReadFileToCharBufferHandler;
import org.egorlitvinenko.testdisruptor.smallstream.util.TestDataProvider;
import org.egorlitvinenko.testdisruptor.smallstream.util.ThreadFactories;
import org.springframework.util.StopWatch;

import java.util.concurrent.ThreadFactory;

/**
 * @author Egor Litvinenko
 */
public class ReadCharBufferMain {

    public static void main(String[] args) throws Exception {
        System.out.println("Enter to start...");
        System.in.read();
        runTest();
    }

    public static void runTest() throws Exception {

        Clickhouse.setApacheHttpClientLoggingSettings();

        ThreadFactory threadFactory = ThreadFactories.simpleDaemonFactory();

        final TestDataProvider.Data testData = TestDataProvider.R_1M__S_1__SQL_DATE_1__I_4__DOUBLE_4__E_0;

        ParseCharBufferCsvDisruptor parseCsvDisruptor = new ParseCharBufferCsvDisruptor();
        Disruptor<ParseCharBufferTableRowEvent> parseCharBufferCsvDisruptorDisruptor =
                parseCsvDisruptor.createWriteToClickhouse(threadFactory,
                        testData.columnTypes,
                        new ClickhouseParseCharBufferTableRowHandler(
                                testData.insert,
                                new SimpleTableRowAndPrepareStatementAdapter()
                        ));


        SimpleTableRowFactory tableRowFactory = new SimpleTableRowFactory(
                testData.columnTypes
        );

        ReadCharBufferDisruptor readCharBufferDisruptor = new ReadCharBufferDisruptor();
        Disruptor<ReadFileToCharBufferEvent> readFileToCharBufferEventDisruptor = readCharBufferDisruptor.create(threadFactory,
                new ReadFileToCharBufferHandler(1024,
                        ',', '\n',
                        '"', testData.columnTypes.length,
                        parseCharBufferCsvDisruptorDisruptor.getRingBuffer(),
                        tableRowFactory));

        StopWatch stopWatch = new StopWatch();
        stopWatch.start("Write CSV to CH");
        readFileToCharBufferEventDisruptor.publishEvent(new EventTranslatorOneArg<ReadFileToCharBufferEvent, String>() {
            @Override
            public void translateTo(ReadFileToCharBufferEvent event, long sequence, String arg0) {
                event.setFile(arg0);
            }
        }, testData.file);
        readFileToCharBufferEventDisruptor.shutdown();
        parseCharBufferCsvDisruptorDisruptor.shutdown();
        stopWatch.stop();
        System.out.println(stopWatch.prettyPrint());


    }

}
