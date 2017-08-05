package org.egorlitvinenko.testdisruptor.byteStreamParsing.reader;

import com.lmax.disruptor.EventTranslator;
import com.lmax.disruptor.EventTranslatorOneArg;
import com.lmax.disruptor.RingBuffer;
import com.univocity.parsers.common.ParsingContext;
import com.univocity.parsers.common.processor.RowProcessor;
import com.univocity.parsers.csv.CsvParser;
import com.univocity.parsers.csv.CsvParserSettings;
import org.egorlitvinenko.testdisruptor.byteStreamParsing.event.ParsePacketEvent;
import org.egorlitvinenko.testdisruptor.byteStreamParsing.util.ColumnType;
import org.springframework.util.StopWatch;

import java.io.File;
import java.io.FileReader;

/**
 * @author Egor Litvinenko
 */
public class UnivosityReaderFromQuotedCsv3 {

    private final char delimiter, quote;
    private final ColumnType[] types;
    private final RingBuffer<ParsePacketEvent> ringBuffer;

    public UnivosityReaderFromQuotedCsv3(char delimiter,
                                         char quote,
                                         ColumnType[] types,
                                         RingBuffer<ParsePacketEvent> ringBuffer) {
        this.delimiter = delimiter;
        this.quote = quote;
        this.types = types;
        this.ringBuffer = ringBuffer;
    }

    public void readFile(String file) throws Exception {

        CsvParserSettings csvParserSettings = new CsvParserSettings();
        csvParserSettings.getFormat().setDelimiter(delimiter);
        csvParserSettings.getFormat().setQuote(quote);
        csvParserSettings.setHeaderExtractionEnabled(true);
        RowProcessor rowProcessor = new TableRowProcessor(this.types, this.ringBuffer);
        csvParserSettings.setProcessor(rowProcessor);

        CsvParser csvParser = new CsvParser(csvParserSettings);
        StopWatch stopWatch = new StopWatch("Parsing csv");
        stopWatch.start();
        csvParser.parse(new FileReader(new File(file)));
        stopWatch.stop();
        System.out.println(stopWatch.prettyPrint());
    }

    public class ParsePacketTranslator implements EventTranslatorOneArg<ParsePacketEvent, String[]> {

        @Override
        public void translateTo(ParsePacketEvent event, long sequence, String[] strings) {
            for (int i = 0; i < strings.length; ++i) {
                switch (UnivosityReaderFromQuotedCsv3.this.types[i]) {
                    case INT_32:
                        event.setInt32String(strings[i], i);
                        break;
                    case DOUBLE:
                        event.setDoubleString(strings[i], i);
                        break;
                    case LOCAL_DATE:
                        event.setLocalDateString(strings[i], i);
                        break;
                    case STRING:
                        event.setString(strings[i], i);
                        break;
                    case SQL_DATE:
                        event.setSqlDateString(strings[i], i);
                        break;
                }
            }
            event.setEnd(Boolean.FALSE);
        }

    }

    private ParsePacketTranslator TRANSLATOR = new ParsePacketTranslator();

    private static EventTranslator<ParsePacketEvent> END_TRANSLATOR = new EventTranslator<ParsePacketEvent>() {
        @Override
        public void translateTo(ParsePacketEvent event, long sequence) {
            event.setEnd(Boolean.TRUE);
        }
    };

    private class TableRowProcessor implements RowProcessor {
        final ColumnType[] types;
        final RingBuffer<ParsePacketEvent> ringBuffer;

        public TableRowProcessor(ColumnType[] types,
                                 RingBuffer<ParsePacketEvent> ringBuffer) {
            this.types = types;
            this.ringBuffer = ringBuffer;

        }

        @Override
        public void processStarted(ParsingContext parsingContext) {
        }

        @Override
        public void rowProcessed(String[] strings, ParsingContext parsingContext) {
            ringBuffer.publishEvent(UnivosityReaderFromQuotedCsv3.this.TRANSLATOR, strings);
        }

        @Override
        public void processEnded(ParsingContext parsingContext) {
            ringBuffer.publishEvent(END_TRANSLATOR);
        }
    }

}
