package org.egorlitvinenko.testdisruptor.smallstream.event;

/**
 * @author Egor Litvinenko
 */
public class NewParsedBatchEvent {

    private int size;
    private Object[][] batch;
    private byte[] types;

    public int getSize() {
        return size;
    }

    public void setSize(int size) {
        this.size = size;
    }

    public Object[][] getBatch() {
        return batch;
    }

    public void setBatch(Object[][] batch) {
        this.batch = batch;
    }

    public byte[] getTypes() {
        return types;
    }

    public void setTypes(byte[] types) {
        this.types = types;
    }
}
