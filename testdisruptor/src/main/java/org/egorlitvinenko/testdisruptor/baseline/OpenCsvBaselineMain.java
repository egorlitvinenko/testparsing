package org.egorlitvinenko.testdisruptor.baseline;

import org.egorlitvinenko.testdisruptor.Clickhouse;
import org.egorlitvinenko.testdisruptor.smallstream.handler.ClickhouseStringBatchSaver;
import com.opencsv.CSVReader;
import org.springframework.util.StopWatch;

import java.io.File;
import java.io.FileReader;
import java.sql.Connection;
import java.sql.PreparedStatement;

/**
 * @author Egor Litvinenko
 */
public class OpenCsvBaselineMain {

    public static void main(String[] args) throws Exception {
        StopWatch stopWatch = new StopWatch();
        stopWatch.start("Write to Clickhouse");
        try(CSVReader csvReader = new CSVReader(new FileReader(new File("test-data.csv")));
            Connection connection = Clickhouse.it().getConnection();
            PreparedStatement ps = connection.prepareStatement(Clickhouse.INSERT_5_STRINGS)) {
            String[] line;
            int counter = 0;
            while (null != (line = csvReader.readNext())) {
                for (int i = 0; i < line.length; ++i) {
                    ps.setString(i + 1, line[i]);
                }
                ps.addBatch();
                if (++counter >= ClickhouseStringBatchSaver.BATCH_SIZE) {
                    ps.executeBatch();
                    counter = 0;
                }
            }
            if (counter > 0) {
                ps.executeBatch();
            }
        }
        stopWatch.stop();
        System.out.println(stopWatch.prettyPrint());
        // >=6.5 sec
    }

}
