package org.egorlitvinenko.testdisruptor.processhandling;

import org.springframework.util.StopWatch;

import java.io.File;

/**
 * @author Egor Litvinenko
 */
public class TestProcessHandle {

    private static final String command =
            "clickhouse-client --query='INSERT INTO test.TEST_DATA_1M_9C_DATE (ID, f1, f2, f3, f4, f5, f6, f7, f8) FORMAT CSVWithNames' < r1m__s1__d4__i4__errors_0.csv";

    public static void main(String[] args) throws Exception {
        int warmup = 0, tests = 1000;
        for (int i = 0; i < warmup; ++i) {
            new ProcessBuilder()
                    .command("/bin/sh", "-c", command)
                    .start();
        }
        StopWatch stopWatch = new StopWatch();
        for (int i = 0; i < tests; ++i) {
            stopWatch.start("Test " + i);
            Process process = new ProcessBuilder()
                    .command("/bin/sh", "-c", command)
                    .start();
            final String error = convertStreamToString(process.getErrorStream());
            if (!error.equals("empty")) {
                throw new RuntimeException();
            }
            stopWatch.stop();
        }
        System.out.println(stopWatch.prettyPrint());
        System.out.println(stopWatch.getTotalTimeMillis() / (stopWatch.getTaskCount() + 0.));
    }

    static String convertStreamToString(java.io.InputStream is) {
        java.util.Scanner s = new java.util.Scanner(is).useDelimiter("\\A");
        return s.hasNext() ? s.next() : "empty";
    }

}
