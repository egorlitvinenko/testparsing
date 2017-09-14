package org.egorlitvinenko.testdisruptor.clickhousestream.reader;

import com.lmax.disruptor.EventTranslator;
import com.lmax.disruptor.EventTranslatorOneArg;
import com.lmax.disruptor.RingBuffer;
import com.univocity.parsers.common.ParsingContext;
import com.univocity.parsers.common.processor.RowProcessor;
import com.univocity.parsers.csv.CsvParser;
import com.univocity.parsers.csv.CsvParserSettings;
import org.egorlitvinenko.testdisruptor.byteStreamParsing.util.ColumnType;
import org.egorlitvinenko.testdisruptor.clickhousestream.event.LineEvent;
import org.springframework.util.StopWatch;

import java.io.File;
import java.io.FileReader;

/**
 * @author Egor Litvinenko
 */
public class UnivosityReaderFromQuotedCsv {

    private final char delimiter, quote;
    private final ColumnType[] types;
    private final RingBuffer<LineEvent> ringBuffer;

    public UnivosityReaderFromQuotedCsv(char delimiter,
                                        char quote,
                                        ColumnType[] types,
                                        RingBuffer<LineEvent> ringBuffer) {
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
        csvParser.parse(new FileReader(new File(file)));
    }

    public class ParsePacketTranslator implements EventTranslatorOneArg<LineEvent, String[]> {

        @Override
        public void translateTo(LineEvent event, long sequence, String[] strings) {
            for (int i = 0; i < strings.length; ++i) {
                event.setValues(strings);
            }
            event.setEndStream(Boolean.FALSE);
        }

    }

    private ParsePacketTranslator TRANSLATOR = new ParsePacketTranslator();

    private static EventTranslator<LineEvent> END_TRANSLATOR = new EventTranslator<LineEvent>() {
        @Override
        public void translateTo(LineEvent event, long sequence) {
            event.setEndStream(Boolean.TRUE);
        }
    };

    private class TableRowProcessor implements RowProcessor {
        final ColumnType[] types;
        final RingBuffer<LineEvent> ringBuffer;

        public TableRowProcessor(ColumnType[] types,
                                 RingBuffer<LineEvent> ringBuffer) {
            this.types = types;
            this.ringBuffer = ringBuffer;

        }

        @Override
        public void processStarted(ParsingContext parsingContext) {
        }

        @Override
        public void rowProcessed(String[] strings, ParsingContext parsingContext) {
            ringBuffer.publishEvent(UnivosityReaderFromQuotedCsv.this.TRANSLATOR, strings);
        }

        @Override
        public void processEnded(ParsingContext parsingContext) {
            ringBuffer.publishEvent(END_TRANSLATOR);
        }
    }

}
