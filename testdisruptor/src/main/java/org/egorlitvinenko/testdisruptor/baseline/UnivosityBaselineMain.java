package org.egorlitvinenko.testdisruptor.baseline;

import com.univocity.parsers.common.ParsingContext;
import com.univocity.parsers.common.processor.RowProcessor;
import com.univocity.parsers.csv.CsvParser;
import com.univocity.parsers.csv.CsvParserSettings;
import org.egorlitvinenko.testdisruptor.Clickhouse;
import org.egorlitvinenko.testdisruptor.smallstream.util.TestDataProvider;
import org.springframework.util.StopWatch;

import java.io.File;
import java.io.FileReader;
import java.sql.Connection;
import java.sql.Date;
import java.sql.PreparedStatement;
import java.time.LocalDate;
import java.time.format.DateTimeFormatter;

/**
 * @author Egor Litvinenko
 */
public class UnivosityBaselineMain {

    private static DateTimeFormatter DATE_FORMATTER = DateTimeFormatter.ISO_LOCAL_DATE;

    public static final int BATCH_SIZE = 15000;
    private static int counter = 0;

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

        Connection connection = Clickhouse.it().getConnection();
        TestDataProvider.Data testData = TestDataProvider.R_1M__S_1__DATE_1__I_4__DOUBLE_4__E_0;
        final PreparedStatement ps = connection.prepareStatement(testData.insert);
        RowProcessor rowProcessor = new RowProcessor() {
            @Override
            public void processStarted(ParsingContext context) {

            }

            @Override
            public void rowProcessed(String[] row, ParsingContext context) {
                try {
                    writeToClickhouse(ps, row);
                } catch (Exception e) {
                    throw new RuntimeException(e);
                }
            }

            @Override
            public void processEnded(ParsingContext context) {

            }
        };

        csvParserSettings.setProcessor(rowProcessor);

        CsvParser csvParser = new CsvParser(csvParserSettings);
        counter = 0;
        StopWatch stopWatch = new StopWatch();
        stopWatch.start();
        csvParser.parse(new FileReader(new File(testData.file)));
        stopWatch.stop();
        System.out.println(stopWatch.prettyPrint());

        // bs - 15000, ~ 12 sec
        // bs - 5000, ~ 12 sec
    }


    public static void writeToClickhouse(PreparedStatement ps, String[] line) throws Exception {
        int i = 0;
        ps.setDate(i + 1, Date.valueOf(LocalDate.from(DATE_FORMATTER.parse(line[i++]))));

        ps.setInt(i + 1, Integer.valueOf(line[i++]));
        ps.setInt(i + 1, Integer.valueOf(line[i++]));
        ps.setInt(i + 1, Integer.valueOf(line[i++]));
        ps.setInt(i + 1, Integer.valueOf(line[i++]));

        ps.setDouble(i + 1, Double.valueOf(line[i++]));
        ps.setDouble(i + 1, Double.valueOf(line[i++]));
        ps.setDouble(i + 1, Double.valueOf(line[i++]));
        ps.setDouble(i + 1, Double.valueOf(line[i++]));

        ps.addBatch();
        if (++counter >= BATCH_SIZE) {
            ps.executeBatch();
            counter = 0;
        }
    }

}
