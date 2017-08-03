package org.egorlitvinenko.testdisruptor.byteStreamParsing.reader;

import org.egorlitvinenko.testdisruptor.byteStreamParsing.event.ParseTableRowEvent;
import org.egorlitvinenko.testdisruptor.byteStreamParsing.factory.TableRowFactory;
import org.egorlitvinenko.testdisruptor.byteStreamParsing.model.TableRow;
import org.egorlitvinenko.testdisruptor.byteStreamParsing.util.ColumnType;
import com.lmax.disruptor.EventTranslatorThreeArg;
import com.lmax.disruptor.RingBuffer;
import com.univocity.parsers.common.ParsingContext;
import com.univocity.parsers.common.processor.RowProcessor;
import com.univocity.parsers.csv.CsvParser;
import com.univocity.parsers.csv.CsvParserSettings;
import org.springframework.util.StopWatch;

import java.io.File;
import java.io.FileReader;

/**
 * @author Egor Litvinenko
 */
public class UnivosityReaderFromQuotedCsv {

    private final char delimiter, quote;
    private final ColumnType[] types;
    private final RingBuffer<ParseTableRowEvent> ringBuffer;

    public UnivosityReaderFromQuotedCsv(char delimiter,
                                        char quote,
                                        ColumnType[] types,
                                        RingBuffer<ParseTableRowEvent> ringBuffer) {
        this.delimiter = delimiter;
        this.quote = quote;
        this.types = types;
        this.ringBuffer = ringBuffer;
    }

    public void readFile(String file, TableRowFactory tableRowFactory) throws Exception {

        CsvParserSettings csvParserSettings = new CsvParserSettings();
        csvParserSettings.getFormat().setDelimiter(delimiter);
        csvParserSettings.getFormat().setQuote(quote);
        csvParserSettings.setHeaderExtractionEnabled(true);
        RowProcessor rowProcessor = new TableRowProcessor(tableRowFactory, this.types, this.ringBuffer);
        csvParserSettings.setProcessor(rowProcessor);

        CsvParser csvParser = new CsvParser(csvParserSettings);
        StopWatch stopWatch = new StopWatch("Parsing csv");
        stopWatch.start();
        csvParser.parse(new FileReader(new File(file)));
        stopWatch.stop();
        System.out.println(stopWatch.prettyPrint());
    }

    private static EventTranslatorThreeArg<ParseTableRowEvent, TableRow, Integer, Boolean> TRANSLATOR = new EventTranslatorThreeArg<ParseTableRowEvent, TableRow, Integer, Boolean>() {
        @Override
        public void translateTo(ParseTableRowEvent event, long sequence, TableRow arg0, Integer arg1, Boolean arg2) {
            event.setTableRow(arg0);
            event.setCurrentColumn(arg1);
            event.setEnd(arg2);
        }
    };

    static class TableRowProcessor implements RowProcessor {
        final TableRowFactory tableRowFactory;
        final ColumnType[] types;
        final RingBuffer<ParseTableRowEvent> ringBuffer;

        public TableRowProcessor(TableRowFactory tableRowFactory,
                                 ColumnType[] types,
                                 RingBuffer<ParseTableRowEvent> ringBuffer) {
            this.tableRowFactory = tableRowFactory;
            this.types = types;
            this.ringBuffer = ringBuffer;
        }

        @Override
        public void processStarted(ParsingContext parsingContext) {
        }

        @Override
        public void rowProcessed(String[] strings, ParsingContext parsingContext) {
            TableRow tableRow = tableRowFactory.create();
            for (int i = 0; i < strings.length; ++i) {
                switch (types[i]) {
                    case INT_32:
                        tableRow.setInt32String(strings[i], i);
                        break;
                    case DOUBLE:
                        tableRow.setDoubleString(strings[i], i);
                        break;
                    case LOCAL_DATE:
                        tableRow.setLocalDateString(strings[i], i);
                        break;
                    case STRING:
                        tableRow.setString(strings[i], i);
                        break;
                }
                ringBuffer.publishEvent(TRANSLATOR, tableRow, i, Boolean.FALSE);
            }
        }

        @Override
        public void processEnded(ParsingContext parsingContext) {
            ringBuffer.publishEvent(TRANSLATOR, null, 0, Boolean.TRUE);
        }
    }

}
