package org.egorlitvinenko.testdisruptor.smallstream.event;

/**
 * @author Egor Litvinenko
 */
public class NewCombinedStringLineEvent {

    private String[] line;

    public String[] getLine() {
        return line;
    }

    public void setLine(String[] line) {
        this.line = line;
    }
}
