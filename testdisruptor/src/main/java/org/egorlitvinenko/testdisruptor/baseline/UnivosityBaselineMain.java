package org.egorlitvinenko.testdisruptor.baseline;

import org.egorlitvinenko.testdisruptor.Clickhouse;
import org.egorlitvinenko.testdisruptor.smallstream.util.TestDataProvider;
import com.univocity.parsers.common.ParsingContext;
import com.univocity.parsers.common.processor.ObjectRowProcessor;
import com.univocity.parsers.csv.CsvParser;
import com.univocity.parsers.csv.CsvParserSettings;
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
        CsvParserSettings csvParserSettings = new CsvParserSettings();
        csvParserSettings.getFormat().setDelimiter(',');
        csvParserSettings.getFormat().setQuote('"');
        csvParserSettings.setHeaderExtractionEnabled(true);

        Connection connection = Clickhouse.it().getConnection();
        final PreparedStatement ps = connection.prepareStatement(TestDataProvider.R_1M__S_1__DATE_1__I_4__DOUBLE_4__E_0.insert);
        ObjectRowProcessor objectRowProcessor = new ObjectRowProcessor() {
            @Override
            public void rowProcessed(Object[] objects, ParsingContext parsingContext) {
                try {
                    writeToClickhouse(ps, objects);
                } catch (Exception e) {
                    throw new RuntimeException(e);
                }
            }
        };

        csvParserSettings.setProcessor(objectRowProcessor);

        CsvParser csvParser = new CsvParser(csvParserSettings);
        counter = 0;
        StopWatch stopWatch = new StopWatch();
        stopWatch.start();
        csvParser.parse(new FileReader(new File(TestDataProvider.R_1M__S_1__DATE_1__I_4__DOUBLE_4__E_0.file)));
        stopWatch.stop();
        System.out.println(stopWatch.prettyPrint());

        // bs - 15000, ~ 12 sec
        // bs - 5000, ~ 12 sec
    }

    public static void writeToClickhouse(PreparedStatement ps, Object[] line) throws Exception {
        int i = 0;
        ps.setDate(i + 1, Date.valueOf(LocalDate.from(DATE_FORMATTER.parse(String.valueOf(line[i++])))));

        ps.setInt(i + 1, Integer.valueOf(line[i++].toString()));
        ps.setInt(i + 1, Integer.valueOf(line[i++].toString()));
        ps.setInt(i + 1, Integer.valueOf(line[i++].toString()));
        ps.setInt(i + 1, Integer.valueOf(line[i++].toString()));

        ps.setDouble(i + 1, Double.valueOf(line[i++].toString()));
        ps.setDouble(i + 1, Double.valueOf(line[i++].toString()));
        ps.setDouble(i + 1, Double.valueOf(line[i++].toString()));
        ps.setDouble(i + 1, Double.valueOf(line[i++].toString()));

        ps.addBatch();
        if (++counter >= BATCH_SIZE) {
            ps.executeBatch();
            counter = 0;
        }
    }

}
