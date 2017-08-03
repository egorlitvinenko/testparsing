package org.egorlitvinenko.testdisruptor.smallstream.event;

/**
 * @author Egor Litvinenko
 */
public class NewStringEvent {

    private String newString;
    private boolean last;

    public boolean isLast() {
        return last;
    }

    public void setLast(boolean last) {
        this.last = last;
    }

    public String getNewString() {
        return newString;
    }

    public void setNewString(String newString) {
        this.newString = newString;
    }
}
