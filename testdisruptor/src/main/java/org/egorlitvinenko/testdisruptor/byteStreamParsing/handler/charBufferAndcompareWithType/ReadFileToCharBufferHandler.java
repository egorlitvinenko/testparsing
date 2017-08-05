package org.egorlitvinenko.testdisruptor.byteStreamParsing.handler.charBufferAndcompareWithType;

import com.google.common.util.concurrent.*;
import com.lmax.disruptor.EventHandler;
import com.lmax.disruptor.EventTranslatorOneArg;
import com.lmax.disruptor.RingBuffer;
import org.egorlitvinenko.testdisruptor.byteStreamParsing.event.ParseCharBufferTableRowEvent;
import org.egorlitvinenko.testdisruptor.byteStreamParsing.event.ReadFileToCharBufferEvent;
import org.egorlitvinenko.testdisruptor.byteStreamParsing.factory.TableRowFactory;
import org.egorlitvinenko.testdisruptor.byteStreamParsing.model.SimpleLine;
import org.egorlitvinenko.testdisruptor.byteStreamParsing.model.TableRow;
import org.egorlitvinenko.testdisruptor.byteStreamParsing.util.ColumnType;

import java.io.File;
import java.io.FileReader;
import java.io.Reader;
import java.util.concurrent.*;

/**
 * @author Egor Litvinenko
 */
public class ReadFileToCharBufferHandler implements EventHandler<ReadFileToCharBufferEvent> {

    private final char delimiter, lineSeparator, quote;
    private final int lineSize;
    private final int bufferSize;
    private final SendParseEvent sendParseEvent;

    private final TranslatorModel translatorModel;
    private final TableRowFactory tableRowFactory;

    private Reader reader;
    private char[] buffer;

    public ReadFileToCharBufferHandler(int bufferSize,
                                       char delimiter,
                                       char lineSeparator,
                                       char quote, int lineSize,
                                       RingBuffer<ParseCharBufferTableRowEvent> ringBuffer,
                                       TableRowFactory tableRowFactory) {
        this.bufferSize = bufferSize;
        this.delimiter = delimiter;
        this.lineSeparator = lineSeparator;
        this.quote = quote;
        this.lineSize = lineSize;
        this.sendParseEvent = new SendParseEvent(ringBuffer);
        this.translatorModel = new TranslatorModel();
        this.tableRowFactory = tableRowFactory;
    }

    @Override
    public void onEvent(ReadFileToCharBufferEvent event, long sequence, boolean endOfBatch) throws Exception {
        try {
            this.reader = new FileReader(new File(event.getFile()));
            int read;
            int[][] map = allocateMap(lineSize);
            buffer = new char[bufferSize];
            int elementCounter = 0;
            boolean isFirst = true;
            StringBuilder lineBuilder = new StringBuilder();
            while (0 < (read = reader.read(buffer, 0, bufferSize))) {
                for (int i = 0; i < read; ++i) {
                    if (isFirst) {
                        if (buffer[i] == lineSeparator) {
                            isFirst = false;
                        }
                    } else {
                        if (buffer[i] == delimiter) {
                            map[elementCounter][SimpleLine.COUNT] =
                                    lineBuilder.length() - map[elementCounter][SimpleLine.START] + 1;

                            lineBuilder.append(buffer[i]);
                            map[++elementCounter][SimpleLine.START] = lineBuilder.length();
                        } else if (buffer[i] == lineSeparator) {
                            map[elementCounter][SimpleLine.COUNT] =
                                    lineBuilder.length() - map[elementCounter][SimpleLine.START] + 1;

                            char[] line = new char[lineBuilder.length()];
                            lineBuilder.getChars(0, line.length, line, 0);
                            sendEvent(line, map, tableRowFactory.create());
                            lineBuilder.setLength(0);
                            elementCounter = 0;
                            map = allocateMap(lineSize);
                        } else if (buffer[i] != quote) {
                            lineBuilder.append(buffer[i]);
                        }
                    }
                }
            }
            sendEnd();
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    private void sendEvent(char[] arg0, int[][] arg1, TableRow tableRow) {
        TranslatorModel model = getTranslatorModel(arg0, arg1, tableRow, ColumnType.DOUBLE);
        try {
            Futures.addCallback(sendParseEvent.apply(model), SendParseEventListener, SEND_PARSE_EXECUTOR);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }

    }

    private void sendEnd() {
        translatorModel.end = true;
        try {
            Futures.addCallback(sendParseEvent.apply(translatorModel), SendParseEventListener);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    public static int[][] allocateMap(int size) {
        int[][] result = new int[size][];
        for (int i = 0; i < result.length; ++i) {
            result[i] = new int[2];
        }
        result[0][SimpleLine.START] = 0;
        return result;
    }

    private static EventTranslatorOneArg<ParseCharBufferTableRowEvent, TranslatorModel> TRANSLATOR =
            new EventTranslatorOneArg<ParseCharBufferTableRowEvent, TranslatorModel>() {
                @Override
                public void translateTo(ParseCharBufferTableRowEvent event, long sequence, TranslatorModel arg0) {
                    event.setChars(arg0.line);
                    event.setMap(arg0.map);
                    event.setTableRow(arg0.tableRow);
                    event.setEnd(arg0.end);
                    event.setType(arg0.type);
                }
            };

    private static class SendParseEvent implements AsyncFunction<TranslatorModel, Boolean> {

        private final RingBuffer<ParseCharBufferTableRowEvent> ringBuffer;
        private final ListeningExecutorService service =
                MoreExecutors.listeningDecorator(Executors.newSingleThreadExecutor());

        public SendParseEvent(RingBuffer<ParseCharBufferTableRowEvent> ringBuffer) {
            this.ringBuffer = ringBuffer;
        }

        @Override
        public ListenableFuture<Boolean> apply(TranslatorModel input) throws Exception {
            return service.submit(new Callable<Boolean>() {
                @Override
                public Boolean call() throws Exception {
                    if (input.end) {
                        ringBuffer.publishEvent(TRANSLATOR, input);
                    } else {
                        if (input.tableRow.hasDoubles()) {
                            input.type = ColumnType.DOUBLE;
                            ringBuffer.publishEvent(TRANSLATOR, input);
                        }
                        if (input.tableRow.hasInt32()) {
                            input.type = ColumnType.INT_32;
                            ringBuffer.publishEvent(TRANSLATOR, input);
                        }
                        if (input.tableRow.hasLocalDates()) {
                            input.type = ColumnType.LOCAL_DATE;
                            ringBuffer.publishEvent(TRANSLATOR, input);
                        }
                        if (input.tableRow.hasSqlDates()) {
                            input.type = ColumnType.SQL_DATE;
                            ringBuffer.publishEvent(TRANSLATOR, input);
                        }
                    }
                    return Boolean.TRUE;
                }
            });
        }
    };

    private static FutureCallback<Boolean> SendParseEventListener = new FutureCallback<Boolean>() {
        @Override
        public void onSuccess(Boolean result) {

        }

        @Override
        public void onFailure(Throwable t) {
            throw new RuntimeException(t);
        }
    };

    private static Executor SEND_PARSE_EXECUTOR = Executors.newFixedThreadPool(2);

    private TranslatorModel getTranslatorModel(char[] arg0, int[][] arg1,
                                    TableRow tableRow, ColumnType type) {
        TranslatorModel translatorModel = new TranslatorModel();
        translatorModel.end = false;
        translatorModel.line = arg0;
        translatorModel.map = arg1;
        translatorModel.tableRow = tableRow;
        translatorModel.type = type;
        return translatorModel;
    }

    private static class TranslatorModel {
        char[] line;
        int[][] map;
        TableRow tableRow;
        ColumnType type;
        boolean end;
    }

}
