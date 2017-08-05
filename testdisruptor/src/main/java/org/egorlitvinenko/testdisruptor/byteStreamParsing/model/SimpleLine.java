package org.egorlitvinenko.testdisruptor.byteStreamParsing.model;

/**
 * @author Egor Litvinenko
 */
public class SimpleLine implements Line {

    public static final int START = 0, COUNT = 1;

    public final int length;

    private char[] chars;
    private int[][] map;

    public SimpleLine(int length) {
        this.length = length;
    }

    public void setChars(char[] chars) {
        this.chars = chars;
    }

    public void setMap(int[][] map) {
        this.map = map;
    }

    public String getValue(int index) {
        try {
            return new String(chars, map[index][START], Math.min(map[index][COUNT], chars.length - map[index][START]));
        } catch (Exception e) {
            throw e;
        }
    }

}
