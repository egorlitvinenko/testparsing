package org.egorlitvinenko.testdisruptor.byteStreamParsing.reader;

import org.egorlitvinenko.testdisruptor.byteStreamParsing.event.ParseBatchTableRowEvent;
import org.egorlitvinenko.testdisruptor.byteStreamParsing.factory.TableRowFactory;
import org.egorlitvinenko.testdisruptor.byteStreamParsing.model.TableRow;
import org.egorlitvinenko.testdisruptor.byteStreamParsing.util.ColumnType;
import com.lmax.disruptor.EventTranslatorOneArg;
import com.lmax.disruptor.RingBuffer;
import com.univocity.parsers.common.ParsingContext;
import com.univocity.parsers.common.processor.RowProcessor;
import com.univocity.parsers.csv.CsvParser;
import com.univocity.parsers.csv.CsvParserSettings;

import java.io.File;
import java.io.FileReader;

/**
 * @author Egor Litvinenko
 */
public class UnivosityBatchReaderFromQuotedCsv {

    private final char delimiter, quote;
    private final ColumnType[] types;
    private final RingBuffer<ParseBatchTableRowEvent> ringBuffer;

    private final int batchSize;

    public UnivosityBatchReaderFromQuotedCsv(int batchSize,
                                             char delimiter,
                                             char quote,
                                             ColumnType[] types,
                                             RingBuffer<ParseBatchTableRowEvent> ringBuffer) {
        this.delimiter = delimiter;
        this.quote = quote;
        this.types = types;
        this.ringBuffer = ringBuffer;
        this.batchSize = batchSize;
    }

    public void readFile(String file, TableRowFactory tableRowFactory) throws Exception {

        CsvParserSettings csvParserSettings = new CsvParserSettings();
        csvParserSettings.getFormat().setDelimiter(delimiter);
        csvParserSettings.getFormat().setQuote(quote);
        csvParserSettings.setHeaderExtractionEnabled(true);
        RowProcessor rowProcessor = new TableRowProcessor(tableRowFactory, this.types, this.batchSize, this.ringBuffer);
        csvParserSettings.setProcessor(rowProcessor);

        CsvParser csvParser = new CsvParser(csvParserSettings);
        csvParser.parse(new FileReader(new File(file)));
    }

    public static EventTranslatorOneArg<ParseBatchTableRowEvent, TranslatorModel> TRANSLATOR =
            new EventTranslatorOneArg<ParseBatchTableRowEvent, TranslatorModel>() {
                @Override
                public void translateTo(ParseBatchTableRowEvent event, long sequence, TranslatorModel model) {
                    event.setTableRows(model.batch);
                    event.setType(model.type);
                    event.setEnd(model.end);
                    event.setSize(model.size);
                }
            };

    public static class TranslatorModel {
        public TableRow[] batch;
        public int size;
        public ColumnType type;
        public Boolean end;
    }

    static class TableRowProcessor implements RowProcessor {
        final TableRowFactory tableRowFactory;
        final ColumnType[] types;
        final RingBuffer<ParseBatchTableRowEvent> ringBuffer;

        final boolean hasDouble, hasInt32, hasLocalDate, hasString, hasSqlDate;

        final int batchSize;

        int rowCounter = 0;

        TableRow[] batch;
        TranslatorModel translatorModel = new TranslatorModel();

        public TableRowProcessor(TableRowFactory tableRowFactory,
                                 ColumnType[] types,
                                 int batchSize,
                                 RingBuffer<ParseBatchTableRowEvent> ringBuffer) {
            this.batchSize = batchSize;
            this.tableRowFactory = tableRowFactory;
            this.types = types;
            this.ringBuffer = ringBuffer;
            this.hasDouble = has(types, ColumnType.DOUBLE);
            this.hasInt32 = has(types, ColumnType.INT_32);
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

        protected void initBatch() {
            if (rowCounter % batchSize == 0) {
                if (rowCounter > 0) {
                    sendEventWith(batch);
                }
                batch = new TableRow[batchSize];
                rowCounter = 0;
            }
        }

        @Override
        public void rowProcessed(String[] strings, ParsingContext parsingContext) {
            initBatch();
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
            batch[rowCounter++] = tableRow;
        }

        protected void sendEventWith(TableRow[] batch) {
            if (hasInt32) {
                setTranslatorModel(batch, ColumnType.INT_32, Boolean.FALSE);
                ringBuffer.publishEvent(TRANSLATOR, translatorModel);
            }
            if (hasDouble) {
                setTranslatorModel(batch, ColumnType.DOUBLE, Boolean.FALSE);
                ringBuffer.publishEvent(TRANSLATOR, translatorModel);
            }
            if (hasString) {
                setTranslatorModel(batch, ColumnType.STRING, Boolean.FALSE);
                ringBuffer.publishEvent(TRANSLATOR, translatorModel);
            }
            if (hasLocalDate) {
                setTranslatorModel(batch, ColumnType.LOCAL_DATE, Boolean.FALSE);
                ringBuffer.publishEvent(TRANSLATOR, translatorModel);
            }
            if (hasSqlDate) {
                setTranslatorModel(batch, ColumnType.SQL_DATE, Boolean.FALSE);
                ringBuffer.publishEvent(TRANSLATOR, translatorModel);
            }
        }

        void setTranslatorModel(TableRow[] batch, ColumnType type, Boolean end) {
            translatorModel.batch = batch;
            translatorModel.type = type;
            translatorModel.end = end;
            translatorModel.size = rowCounter;
        }

        @Override
        public void processEnded(ParsingContext parsingContext) {
            if (rowCounter > 0) {
                sendEventWith(batch);
            }
            setTranslatorModel(null, ColumnType.LOCAL_DATE, Boolean.TRUE);
            ringBuffer.publishEvent(TRANSLATOR, translatorModel);
        }
    }

}
