package org.egorlitvinenko.testdisruptor.byteStreamParsing;

import org.egorlitvinenko.testdisruptor.Clickhouse;
import org.egorlitvinenko.testdisruptor.byteStreamParsing.disruptor.ParsedTableRowDisruptor;
import org.egorlitvinenko.testdisruptor.byteStreamParsing.disruptor.StringToParseDoubleDisruptor;
import org.egorlitvinenko.testdisruptor.byteStreamParsing.disruptor.StringToParseInt32Disruptor;
import org.egorlitvinenko.testdisruptor.byteStreamParsing.disruptor.StringToParseLocalDateDisruptor;
import org.egorlitvinenko.testdisruptor.byteStreamParsing.event.ParsedTableRowEvent;
import org.egorlitvinenko.testdisruptor.byteStreamParsing.event.StringToParseDoubleEvent;
import org.egorlitvinenko.testdisruptor.byteStreamParsing.event.StringToParseInt32Event;
import org.egorlitvinenko.testdisruptor.byteStreamParsing.event.StringToParseLocalDateEvent;
import org.egorlitvinenko.testdisruptor.byteStreamParsing.factory.SimpleTableRowFactory;
import org.egorlitvinenko.testdisruptor.byteStreamParsing.handler.CountParsedTableRowHandler;
import org.egorlitvinenko.testdisruptor.byteStreamParsing.handler.splittedByEvent.StringToDoubleParseHandler;
import org.egorlitvinenko.testdisruptor.byteStreamParsing.handler.splittedByEvent.StringToInt32ParseHandler;
import org.egorlitvinenko.testdisruptor.byteStreamParsing.handler.splittedByEvent.StringToLocalDateParseHandler;
import org.egorlitvinenko.testdisruptor.byteStreamParsing.parsing.spi.SimpleDoubleValueOf;
import org.egorlitvinenko.testdisruptor.byteStreamParsing.parsing.spi.SimpleIntegerValueOf;
import org.egorlitvinenko.testdisruptor.byteStreamParsing.parsing.spi.SimpleIsoLocalDateParser;
import org.egorlitvinenko.testdisruptor.byteStreamParsing.publisher.DoubleParsePublisher;
import org.egorlitvinenko.testdisruptor.byteStreamParsing.publisher.GroupRingBuffers;
import org.egorlitvinenko.testdisruptor.byteStreamParsing.publisher.Int32ParsePublisher;
import org.egorlitvinenko.testdisruptor.byteStreamParsing.publisher.LocalDateParsePublisher;
import org.egorlitvinenko.testdisruptor.byteStreamParsing.reader.ByteStreamReaderFromQuotedCsv;
import org.egorlitvinenko.testdisruptor.smallstream.util.TestDataProvider;
import org.egorlitvinenko.testdisruptor.smallstream.util.ThreadFactories;
import com.lmax.disruptor.dsl.Disruptor;
import org.springframework.util.StopWatch;

import java.util.concurrent.ThreadFactory;

/**
 * @author Egor Litvinenko
 */
public class ByteStream5SParsingCounterMain {

    public static void main(String[] args) throws Exception {
        System.out.println("Enter to start...");
        System.in.read();
        runTest();
    }

    public static void runTest() throws Exception {

        Clickhouse.setApacheHttpClientLoggingSettings();

        ThreadFactory threadFactory = ThreadFactories.simpleDaemonFactory();

        CountParsedTableRowHandler counterHandler = new CountParsedTableRowHandler();
        Disruptor<ParsedTableRowEvent> parsedTableRowEventDisruptor = new ParsedTableRowDisruptor()
                .create(
                        threadFactory,
                        counterHandler);

        StringToDoubleParseHandler stringToDoubleParseHandler = new StringToDoubleParseHandler(
                new SimpleDoubleValueOf(),
                parsedTableRowEventDisruptor.getRingBuffer());
        Disruptor<StringToParseDoubleEvent> stringToParseDoubleEventDisruptor =
                new StringToParseDoubleDisruptor().create(threadFactory, stringToDoubleParseHandler);

        StringToInt32ParseHandler stringToInt32ParseHandler = new StringToInt32ParseHandler(
                new SimpleIntegerValueOf(),
                parsedTableRowEventDisruptor.getRingBuffer());
        Disruptor<StringToParseInt32Event> stringToParseInt32EventDisruptor =
                new StringToParseInt32Disruptor().create(threadFactory, stringToInt32ParseHandler);

        StringToLocalDateParseHandler stringToLocalDateParseHandler = new StringToLocalDateParseHandler(
                new SimpleIsoLocalDateParser(),
                parsedTableRowEventDisruptor.getRingBuffer());
        Disruptor<StringToParseLocalDateEvent> stringToParseLocalDateEventDisruptor =
                new StringToParseLocalDateDisruptor().create(
                        threadFactory, stringToLocalDateParseHandler);

        DoubleParsePublisher doubleParsePublisher =
                new DoubleParsePublisher(stringToParseDoubleEventDisruptor.getRingBuffer());
        Int32ParsePublisher int32ParsePublisher =
                new Int32ParsePublisher(stringToParseInt32EventDisruptor.getRingBuffer());
        LocalDateParsePublisher localDateParsePublisher =
                new LocalDateParsePublisher(stringToParseLocalDateEventDisruptor.getRingBuffer());

        GroupRingBuffers groupRingBuffers = new GroupRingBuffers(
                doubleParsePublisher,
                int32ParsePublisher,
                localDateParsePublisher);

        ByteStreamReaderFromQuotedCsv reader = new ByteStreamReaderFromQuotedCsv((byte) '\n', (byte) ',', (byte) '"',
                TestDataProvider.R_1M__S_1__DATE_1__I_4__DOUBLE_4__E_0.columnTypes, groupRingBuffers);

        SimpleTableRowFactory tableRowFactory = new SimpleTableRowFactory(
                TestDataProvider.R_1M__S_1__DATE_1__I_4__DOUBLE_4__E_0.columnTypes
        );

        StopWatch stopWatch = new StopWatch();
        stopWatch.start("Parsing rows");
        reader.readFile(TestDataProvider.R_1M__S_1__DATE_1__I_4__DOUBLE_4__E_0.file,
                tableRowFactory);
        stringToParseInt32EventDisruptor.shutdown();
        stringToParseDoubleEventDisruptor.shutdown();
        stringToParseLocalDateEventDisruptor.shutdown();
        parsedTableRowEventDisruptor.shutdown();
        stopWatch.stop();
        System.out.println(stopWatch.prettyPrint());
        System.out.println(counterHandler.getCounter());
        System.out.println(counterHandler.getCalls());
        System.out.println(stringToLocalDateParseHandler.getCounter());
        System.out.println(stringToDoubleParseHandler.getCounter());
        System.out.println(stringToInt32ParseHandler.getCounter());
        System.out.println(stringToLocalDateParseHandler.getCounter2());
        System.out.println(stringToDoubleParseHandler.getCounter2());
        System.out.println(stringToInt32ParseHandler.getCounter2());

    }

}
