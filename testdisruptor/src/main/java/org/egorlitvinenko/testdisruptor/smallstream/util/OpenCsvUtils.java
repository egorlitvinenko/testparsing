package org.egorlitvinenko.testdisruptor.smallstream.util;

import org.egorlitvinenko.testdisruptor.smallstream.event.NewBatchEvent;
import com.lmax.disruptor.EventTranslatorTwoArg;
import com.lmax.disruptor.dsl.Disruptor;
import com.opencsv.CSVReader;

import java.io.File;
import java.io.FileReader;

/**
 * @author Egor Litvinenko
 */
public class OpenCsvUtils {

    public static void readCsvFileWithBatches(String file,
                                              Disruptor<NewBatchEvent> newBatchEventDisruptor,
                                              int batchSize, boolean copyBatches) throws Exception {
        final String[][] batch = new String[batchSize][];
        final EventTranslatorTwoArg<NewBatchEvent, String[][], Integer> translator = copyBatches ?
                NewBatchEvent.COPY_TRANSLATOR :
                NewBatchEvent.TRANSLATOR;

        try(CSVReader csvReader = new CSVReader(new FileReader(new File(file)),
                ',', '"')) {
            String[] line;
            int read = 0;
            boolean isFirstLine = true;
            while (null != (line = csvReader.readNext())) {
                if (!isFirstLine) {
                    batch[read++] = line;
                    if (read >= batchSize) {
                        newBatchEventDisruptor.getRingBuffer().publishEvent(translator, batch, read);
                        read = 0;
                    }
                } else {
                    isFirstLine = false;
                }
            }
            if (read > 0) {
                newBatchEventDisruptor.getRingBuffer().publishEvent(translator, batch, read);
            }
        }
    }


}
