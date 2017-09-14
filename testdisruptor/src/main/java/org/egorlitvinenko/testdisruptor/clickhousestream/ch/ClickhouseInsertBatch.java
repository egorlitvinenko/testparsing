package org.egorlitvinenko.testdisruptor.clickhousestream.ch;

/**
 * @author Egor Litvinenko
 */
public class ClickhouseInsertBatch {

    private static final String TAB_SEPARATED = "TabSeparated";

    private final ClickhouseHttp clickhouseHttp;
    private final String insert;
    private final PreparedStream preparedStream;
    private final int batchSize;

    private int counter;

    public ClickhouseInsertBatch(ClickhouseHttp clickhouseHttp,
                                 String sql,
                                 int batchSize,
                                 PreparedStream preparedStream) {
        this.clickhouseHttp = clickhouseHttp;
        this.insert = sql + " FORMAT " + TAB_SEPARATED;
        this.preparedStream = preparedStream;
        this.batchSize = batchSize;
    }

    public void addBatch(String[] line) throws Exception {
        for (int i = 0; i < line.length - 1; ++i) {
            preparedStream.appendValue(line[i]);
        }
        preparedStream.appendLastValue(line[line.length - 1]);
        ++counter;
    }

    public boolean readyToBatch() {
        return counter % batchSize == 0;
    }

    public void endBatching() throws Exception {
        if (counter > 0) {
            execute();
        }
    }

    public void execute() throws Exception {
        InsertEntity insertEntity = InsertEntity.of(preparedStream);
        clickhouseHttp.sendInsert(insertEntity, insert);
        preparedStream.clear();
        counter = 0;
    }

}
