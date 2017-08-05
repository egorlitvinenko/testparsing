package org.egorlitvinenko.testdisruptor.byteStreamParsing.event;

/**
 * @author Egor Litvinenko
 */
public class ReadFileToCharBufferEvent {

    private String file;

    public String getFile() {
        return file;
    }

    public void setFile(String file) {
        this.file = file;
    }
}
