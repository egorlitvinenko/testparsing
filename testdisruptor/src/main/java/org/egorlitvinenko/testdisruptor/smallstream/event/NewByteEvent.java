package org.egorlitvinenko.testdisruptor.smallstream.event;

/**
 * @author Egor Litvinenko
 */
public class NewByteEvent {

    private byte character;

    public void setCharacter(byte character) {
        this.character = character;
    }

    public byte getCharacter() {
        return character;
    }
}
