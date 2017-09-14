package org.egorlitvinenko.testdisruptor.clickhousestream;

import com.univocity.parsers.common.ParsingContext;
import com.univocity.parsers.common.processor.RowProcessor;
import com.univocity.parsers.csv.CsvParser;
import com.univocity.parsers.csv.CsvParserSettings;
import org.egorlitvinenko.testdisruptor.byteStreamParsing.parsing.PositionedIsoDateParser;
import org.egorlitvinenko.testdisruptor.clickhousestream.ch.ClickhouseHttp;
import org.egorlitvinenko.testdisruptor.clickhousestream.ch.ClickhouseInsertBatch;
import org.egorlitvinenko.testdisruptor.clickhousestream.ch.ClickhousePreparedStream;
import org.egorlitvinenko.testdisruptor.smallstream.util.TestDataProvider;
import org.springframework.util.StopWatch;

import java.io.File;
import java.io.FileReader;

/**
 * @author Egor Litvinenko
 */
public class UnivosityBaselineMain {


    private static PositionedIsoDateParser positionedIsoDateParser = new PositionedIsoDateParser();

    public static final int BATCH_SIZE = 15000;

    public static void main(String[] args) throws Exception {
        System.out.println("Enter to start...");
        System.in.read();
        runTest();
    }

    public static void runTest() throws Exception {

        CsvParserSettings csvParserSettings = new CsvParserSettings();
        csvParserSettings.getFormat().setDelimiter(',');
        csvParserSettings.getFormat().setQuote('"');
        csvParserSettings.setHeaderExtractionEnabled(true);

        ClickhouseHttp clickhouseHttp = new ClickhouseHttp("jdbc:clickhouse://localhost:9123");
        ClickhouseInsertBatch insertBatch = new ClickhouseInsertBatch(clickhouseHttp,
                "INSERT INTO test.TEST_DATA_1M_9C_DATE (ID, f1, f2, f3, f4, f5, f6, f7, f8)",
                BATCH_SIZE,
                new ClickhousePreparedStream(BATCH_SIZE * 1000, false));

        TestDataProvider.Data testData = TestDataProvider.R_1M__S_1__DATE_1__I_4__DOUBLE_4__E_0;
        RowProcessor rowProcessor = new RowProcessor() {
            @Override
            public void processStarted(ParsingContext context) {

            }

            @Override
            public void rowProcessed(String[] row, ParsingContext context) {
                try {
                    writeToClickhouse(insertBatch, row);
                } catch (Exception e) {
                    throw new RuntimeException(e);
                }
            }

            @Override
            public void processEnded(ParsingContext context) {
                try {
                    insertBatch.endBatching();
                } catch (Exception e) {
                    throw new RuntimeException(e);
                }
            }
        };

        csvParserSettings.setProcessor(rowProcessor);

        CsvParser csvParser = new CsvParser(csvParserSettings);
        StopWatch stopWatch = new StopWatch();
        stopWatch.start();
        csvParser.parse(new FileReader(new File(testData.file)));
        stopWatch.stop();
        System.out.println(stopWatch.prettyPrint());

        // bs - 15000, ~ 12 sec
        // bs - 5000, ~ 12 sec
    }


    public static void writeToClickhouse(ClickhouseInsertBatch insertBatch, String[] line) throws Exception {
        if (isValid(line)) {
            insertBatch.addBatch(line);
            if (insertBatch.readyToBatch()) {
                insertBatch.execute();
            }
        }
    }

    public static boolean isValid(String[] line) {
        try {
            int i = 0;
            positionedIsoDateParser.parse(line[i++]);
            Integer.parseInt(line[i++]);
            Integer.parseInt(line[i++]);
            Integer.parseInt(line[i++]);
            Integer.parseInt(line[i++]);
            Double.parseDouble(line[i++]);
            Double.parseDouble(line[i++]);
            Double.parseDouble(line[i++]);
            Double.parseDouble(line[i++]);
            return true;
        } catch (Exception e) {
            return false;
        }

    }

}
