package org.egorlitvinenko.testdisruptor.clickhousestream.ch;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.charset.StandardCharsets;

/**
 * @author Egor Litvinenko
 */
public class ClickhousePreparedStream implements PreparedStream {

    private static final byte LINE_END = '\n';
    private static final byte VALUE_SPLITTER = '\t';

    private ByteBuffer byteBuffer;

    public ClickhousePreparedStream(int capacity) {
        this(capacity, false);
    }

    public ClickhousePreparedStream(int capacity, boolean offHeap) {
        this.byteBuffer = offHeap
                ? ByteBuffer.allocateDirect(capacity).order(ByteOrder.LITTLE_ENDIAN)
                : ByteBuffer.allocate(capacity).order(ByteOrder.LITTLE_ENDIAN);
    }

    public void appendRow(String value) {
        byteBuffer.put(value.getBytes(StandardCharsets.UTF_8));
        byteBuffer.put(LINE_END);
    }

    public void appendValue(String value) {
        byteBuffer.put(value.getBytes(StandardCharsets.UTF_8));
        byteBuffer.put(VALUE_SPLITTER);
    }

    public void appendLastValue(String value) {
        byteBuffer.put(value.getBytes(StandardCharsets.UTF_8));
        byteBuffer.put(LINE_END);
    }

    public byte[] getBytes() {
        byteBuffer.flip();
        final byte[] bb = new byte[byteBuffer.remaining()];
        byteBuffer.get(bb);
        return bb;
    }

    public void clear() {
        byteBuffer.clear();
    }

    @Override
    public void close() throws Exception {
        byteBuffer = null;
    }
}
