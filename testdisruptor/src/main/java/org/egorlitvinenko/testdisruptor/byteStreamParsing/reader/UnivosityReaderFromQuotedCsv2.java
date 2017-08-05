package org.egorlitvinenko.testdisruptor.byteStreamParsing.reader;

import org.egorlitvinenko.testdisruptor.byteStreamParsing.event.ParsePacketTableRowEvent;
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
public class UnivosityReaderFromQuotedCsv2 {

    private final char delimiter, quote;
    private final ColumnType[] types;
    private final RingBuffer<ParsePacketTableRowEvent> ringBuffer;

    public UnivosityReaderFromQuotedCsv2(char delimiter,
                                         char quote,
                                         ColumnType[] types,
                                         RingBuffer<ParsePacketTableRowEvent> ringBuffer) {
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

    public static EventTranslatorThreeArg<ParsePacketTableRowEvent, TableRow, ColumnType, Boolean> TRANSLATOR =
            new EventTranslatorThreeArg<ParsePacketTableRowEvent, TableRow, ColumnType, Boolean>() {
        @Override
        public void translateTo(ParsePacketTableRowEvent event, long sequence, TableRow arg0, ColumnType arg1, Boolean arg2) {
            event.setTableRow(arg0);
            event.setType(arg1);
            event.setEnd(arg2);
        }
    };

    static class TableRowProcessor implements RowProcessor {
        final TableRowFactory tableRowFactory;
        final ColumnType[] types;
        final RingBuffer<ParsePacketTableRowEvent> ringBuffer;

        final boolean hasDouble, hasInt32, hasLocalDate, hasString, hasSqlDate;

        public TableRowProcessor(TableRowFactory tableRowFactory,
                                 ColumnType[] types,
                                 RingBuffer<ParsePacketTableRowEvent> ringBuffer) {
            this.tableRowFactory = tableRowFactory;
            this.types = types;
            this.ringBuffer = ringBuffer;
            this.hasDouble = has(types, ColumnType.DOUBLE);
            this.hasInt32  = has(types, ColumnType.INT_32);
            this.hasString = has(types, ColumnType.STRING);
            this.hasLocalDate = has(types, ColumnType.LOCAL_DATE);
            this.hasSqlDate = has(types, ColumnType.SQL_DATE);

        }

        private boolean has(ColumnType[] types, ColumnType type) {
            for (int i = 0; i < types.length; ++i) {
                if (types[i] == type) {
                    return true;
                }
            }
            return false;
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
                    case SQL_DATE:
                        tableRow.setSqlDateString(strings[i], i);
                        break;
                }
            }
            // TODO publishEvents?
            if (hasInt32) {
                ringBuffer.publishEvent(TRANSLATOR, tableRow, ColumnType.INT_32, Boolean.FALSE);
            }
            if (hasDouble) {
                ringBuffer.publishEvent(TRANSLATOR, tableRow, ColumnType.DOUBLE, Boolean.FALSE);
            }
            if (hasString) {
                ringBuffer.publishEvent(TRANSLATOR, tableRow, ColumnType.STRING, Boolean.FALSE);
            }
            if (hasLocalDate) {
                ringBuffer.publishEvent(TRANSLATOR, tableRow, ColumnType.LOCAL_DATE, Boolean.FALSE);
            }
            if (hasSqlDate) {
                ringBuffer.publishEvent(TRANSLATOR, tableRow, ColumnType.SQL_DATE, Boolean.FALSE);
            }
        }

        @Override
        public void processEnded(ParsingContext parsingContext) {
            ringBuffer.publishEvent(TRANSLATOR, null, ColumnType.INT_32, Boolean.TRUE);
        }
    }

}
