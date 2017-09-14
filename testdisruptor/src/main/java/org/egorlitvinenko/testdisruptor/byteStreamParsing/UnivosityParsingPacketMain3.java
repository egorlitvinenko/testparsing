package org.egorlitvinenko.testdisruptor.byteStreamParsing;

import com.lmax.disruptor.EventTranslator;
import com.lmax.disruptor.dsl.Disruptor;
import org.egorlitvinenko.testdisruptor.Clickhouse;
import org.egorlitvinenko.testdisruptor.byteStreamParsing.adapter.SimpleTableRowAndPrepareStatementAdapter;
import org.egorlitvinenko.testdisruptor.byteStreamParsing.disruptor.ParsePacketDisruptor;
import org.egorlitvinenko.testdisruptor.byteStreamParsing.event.ParsePacketEvent;
import org.egorlitvinenko.testdisruptor.byteStreamParsing.handler.parsePacket.ClickhouseParsePacketHandler;
import org.egorlitvinenko.testdisruptor.smallstream.util.TestDataProvider;
import org.egorlitvinenko.testdisruptor.smallstream.util.ThreadFactories;
import org.springframework.util.StopWatch;

import java.util.concurrent.ThreadFactory;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * @author Egor Litvinenko
 */
public class UnivosityParsingPacketMain3 {

    public static void main(String[] args) throws Exception {
        System.out.println("Enter to start...");
        System.in.read();
        runTestWithReadFile();
    }

    public static void runTestWithReadFile() throws Exception {

        Clickhouse.setApacheHttpClientLoggingSettings();

        ThreadFactory threadFactory = ThreadFactories.simpleDaemonFactory();

        final TestDataProvider.Data testData = TestDataProvider.R_1M__S_1__SQL_DATE_1__I_4__DOUBLE_4__E_0;

        AtomicBoolean isEnd = new AtomicBoolean(Boolean.FALSE);
        ParsePacketDisruptor parseCsvDisruptor = new ParsePacketDisruptor(testData.columnTypes);
        Disruptor<ParsePacketEvent> parsePacketTableRowEventDisruptor =
                parseCsvDisruptor.createReadAndWriteToClickhouse(threadFactory,
                        testData.file,
                        ',',
                        '"',
                        aBoolean -> isEnd.set(aBoolean),
                        testData.columnTypes,
                        new ClickhouseParsePacketHandler(
                                testData.insert,
                                15000,
                                new SimpleTableRowAndPrepareStatementAdapter()
                        ));


        StopWatch common = new StopWatch();
        common.start("Write CSV to CH - All");
        StopWatch stopWatch = new StopWatch();
        for (int i = 0; i < 1; ++i) {
            stopWatch.start("Write CSV to CH - " + i);
            while(!isEnd.get()) {
                parsePacketTableRowEventDisruptor.publishEvent(TRANSLATOR);
            }
            stopWatch.stop();
        }
        parsePacketTableRowEventDisruptor.shutdown();
        common.stop();
        System.out.println(stopWatch.prettyPrint());
        System.out.println(common.prettyPrint());
    }

    private static EventTranslator<ParsePacketEvent> TRANSLATOR = new EventTranslator<ParsePacketEvent>() {
        @Override
        public void translateTo(ParsePacketEvent event, long sequence) {

        }
    };

}
