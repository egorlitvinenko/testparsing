package org.egorlitvinenko.testdisruptor.byteStreamParsing.reader;

import org.egorlitvinenko.testdisruptor.byteStreamParsing.event.ParseTableRowEvent;
import org.egorlitvinenko.testdisruptor.byteStreamParsing.factory.TableRowFactory;
import org.egorlitvinenko.testdisruptor.byteStreamParsing.model.TableRow;
import org.egorlitvinenko.testdisruptor.byteStreamParsing.util.ColumnType;
import com.lmax.disruptor.EventTranslatorTwoArg;
import com.lmax.disruptor.RingBuffer;

import java.nio.ByteBuffer;
import java.nio.channels.SeekableByteChannel;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Paths;

/**
 * @author Egor Litvinenko
 */
public class ByteStreamReaderFromQuotedCsv2 {

    private int elements = 0, rows = 0;

    private final ByteBuffer aggregate;
    private int aggregateCounter;

    private final byte delimiter, lineEnd, quote;
    private final Charset charset = StandardCharsets.UTF_8;
    private final int lineSize;
    private final ColumnType[] types;
    private final RingBuffer<ParseTableRowEvent> ringBuffer;

    private int valueCounter = 0;

    public ByteStreamReaderFromQuotedCsv2(byte lineEnd,
                                          byte delimiter,
                                          byte quote,
                                          ColumnType[] types,
                                          RingBuffer<ParseTableRowEvent> ringBuffer) {
        this.aggregate = ByteBuffer.allocate(100);
        this.delimiter = delimiter;
        this.lineEnd = lineEnd;
        this.quote = quote;
        this.types = types;
        this.lineSize = types.length;
        this.ringBuffer = ringBuffer;
    }

    public void readFile(String file, TableRowFactory tableRowFactory) throws Exception {
        SeekableByteChannel fileChannel = Files.newByteChannel(Paths.get(file));
        int bufferSize = 1024 * 512;
        ByteBuffer byteBuffer = ByteBuffer.allocate(bufferSize);
        int read, processed = 0;
        byte character;
        TableRow tableRow = tableRowFactory.create();
        boolean isFirst = true;
        int rows = 0;
        while ((read = fileChannel.read(byteBuffer)) > 0) {
            byteBuffer.rewind();
            byteBuffer.limit(read);
            for (int i = 0; i < read; ++i) {
                character = byteBuffer.get();
                if (!isFirst) {
                    handleByte(tableRow, character);
                }
                if (character == lineEnd) {
                    tableRow = tableRowFactory.create();
                    isFirst = false;
                    rows++;
                }
                processed++;
            }
            byteBuffer.flip();
        }
        System.out.println("Read is " + read);
        System.out.println("Processed is " + processed);
        System.out.println("Rows are " + rows);
        System.out.println("Elements are " + elements);
    }

    public void handleByte(TableRow tableRow, byte character) throws Exception {
        if (character == delimiter || character == lineEnd) {
            handleString(tableRow, getString(aggregate, aggregateCounter, charset));
            this.aggregateCounter = 0;
            ++this.elements;
        } else if (character != quote) {
            aggregate.put(aggregateCounter++, character);
        }
    }

    private void handleString(TableRow tableRow, String newString) {
        switch (types[valueCounter]) {
            case INT_32:
                tableRow.setInt32String(newString, valueCounter);
                break;
            case DOUBLE:
                tableRow.setDoubleString(newString, valueCounter);
                break;
            case LOCAL_DATE:
                tableRow.setLocalDateString(newString, valueCounter);
                break;
            case STRING:
                tableRow.setString(newString, valueCounter);
                break;
            default:
                throw new RuntimeException("Unknownk type: " + types[valueCounter]);
        }
        ringBuffer.publishEvent(TRANSLATOR, tableRow, valueCounter);
        valueCounter++;
        if (valueCounter == lineSize) {
            valueCounter = 0;
        }
    }

    private static EventTranslatorTwoArg<ParseTableRowEvent, TableRow, Integer> TRANSLATOR = new EventTranslatorTwoArg<ParseTableRowEvent, TableRow, Integer>() {
        @Override
        public void translateTo(ParseTableRowEvent event, long sequence, TableRow arg0, Integer arg1) {
            event.setTableRow(arg0);
            event.setCurrentColumn(arg1);
        }
    };

    private static String getString(ByteBuffer byteBuffer, int length, Charset charset) {
        byte[] temp = new byte[length];
        System.arraycopy(byteBuffer.array(), 0, temp, 0, length);
        return new String(temp, charset);
    }

}
