package org.egorlitvinenko.testdisruptor.byteStreamParsing.reader;

import org.egorlitvinenko.testdisruptor.byteStreamParsing.event.ParsePacketTableRowEvent;
import org.egorlitvinenko.testdisruptor.byteStreamParsing.factory.TableRowFactory;
import org.egorlitvinenko.testdisruptor.byteStreamParsing.model.TableRow;
import org.egorlitvinenko.testdisruptor.byteStreamParsing.util.ColumnType;
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
public class ByteStreamPacketReaderFromQuotedCsv {

    private int elements = 0, rows = 0;

    private final ByteBuffer aggregate;
    private int aggregateCounter;

    private final byte delimiter, lineEnd, quote;
    private final Charset charset = StandardCharsets.UTF_8;
    private final ColumnType[] types;
    private final RingBuffer<ParsePacketTableRowEvent> ringBuffer;

    private int valueCounter = 0,
            doubleCounter = 0,
            localDateCounter = 0,
            int32Counter = 0;

    private final int DOUBLE_COUNT,
            INT_32_COUNT,
            LOCAL_DATE_COUNT;

    public ByteStreamPacketReaderFromQuotedCsv(byte lineEnd,
                                               byte delimiter,
                                               byte quote,
                                               ColumnType[] types,
                                               RingBuffer<ParsePacketTableRowEvent> ringBuffer) {
        this.aggregate = ByteBuffer.allocate(100);
        this.delimiter = delimiter;
        this.lineEnd = lineEnd;
        this.quote = quote;
        this.types = types;
        this.ringBuffer = ringBuffer;

        this.DOUBLE_COUNT = ColumnType.count(types, ColumnType.DOUBLE);
        this.INT_32_COUNT = ColumnType.count(types, ColumnType.INT_32);
        this.LOCAL_DATE_COUNT = ColumnType.count(types, ColumnType.LOCAL_DATE);

    }

    public void readFile(String file, TableRowFactory tableRowFactory) throws Exception {
        SeekableByteChannel fileChannel = Files.newByteChannel(Paths.get(file));
        int bufferSize = 1024 * 512;
        ByteBuffer byteBuffer = ByteBuffer.allocate(bufferSize);
        int read, processed = 0;
        byte character;
        TableRow tableRow = tableRowFactory.create();
        boolean isFirst = true;
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
                    doubleCounter = 0;
                    localDateCounter = 0;
                    int32Counter = 0;
                    valueCounter = 0;
                    isFirst = false;
                    rows++;
                }
                processed++;
            }
            byteBuffer.flip();
        }
        ringBuffer.publishEvent(UnivosityReaderFromQuotedCsv2.TRANSLATOR, null, ColumnType.LOCAL_DATE, Boolean.TRUE);
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
                int32Counter++;
                if (int32Counter == INT_32_COUNT) {
                    ringBuffer.publishEvent(UnivosityReaderFromQuotedCsv2.TRANSLATOR, tableRow, ColumnType.INT_32, Boolean.FALSE);
                }
                break;
            case DOUBLE:
                tableRow.setDoubleString(newString, valueCounter);
                doubleCounter++;
                if (doubleCounter == DOUBLE_COUNT) {
                    ringBuffer.publishEvent(UnivosityReaderFromQuotedCsv2.TRANSLATOR, tableRow, ColumnType.DOUBLE, Boolean.FALSE);
                }
                break;
            case LOCAL_DATE:
                tableRow.setLocalDateString(newString, valueCounter);
                localDateCounter++;
                if (localDateCounter == LOCAL_DATE_COUNT) {
                    ringBuffer.publishEvent(UnivosityReaderFromQuotedCsv2.TRANSLATOR, tableRow, ColumnType.LOCAL_DATE, Boolean.FALSE);
                }
                break;
            case STRING:
                tableRow.setString(newString, valueCounter);
                break;
            default:
                throw new RuntimeException("Unknown type: " + types[valueCounter]);
        }
        valueCounter++;
    }

    private static String getString(ByteBuffer byteBuffer, int length, Charset charset) {
        byte[] temp = new byte[length];
        System.arraycopy(byteBuffer.array(), 0, temp, 0, length);
        return new String(temp, charset);
    }

}
