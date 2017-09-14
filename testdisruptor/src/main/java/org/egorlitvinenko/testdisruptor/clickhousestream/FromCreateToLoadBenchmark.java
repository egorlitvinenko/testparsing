package org.egorlitvinenko.testdisruptor.clickhousestream;

import com.lmax.disruptor.dsl.Disruptor;
import org.egorlitvinenko.testdisruptor.Clickhouse;
import org.egorlitvinenko.testdisruptor.clickhousestream.ch.BufferPreparedStream;
import org.egorlitvinenko.testdisruptor.clickhousestream.ch.ClickhouseHttp;
import org.egorlitvinenko.testdisruptor.clickhousestream.ch.ClickhouseInsertBatch;
import org.egorlitvinenko.testdisruptor.clickhousestream.disruptor.LineEventDisruptor;
import org.egorlitvinenko.testdisruptor.clickhousestream.event.LineEvent;
import org.egorlitvinenko.testdisruptor.clickhousestream.reader.UnivosityReaderFromQuotedCsv;
import org.egorlitvinenko.testdisruptor.smallstream.util.TestDataProvider;
import org.egorlitvinenko.testdisruptor.smallstream.util.ThreadFactories;
import org.openjdk.jmh.annotations.*;
import org.openjdk.jmh.runner.Runner;
import org.openjdk.jmh.runner.options.OptionsBuilder;

import java.sql.SQLException;
import java.sql.Statement;
import java.util.concurrent.ThreadFactory;

import static org.egorlitvinenko.testdisruptor.clickhousestream.UnivosityDisruptorBaselineMain.BATCH_SIZE;

/**
 * @author Egor Litvinenko
 */
@State(Scope.Benchmark)
public class FromCreateToLoadBenchmark {

    ClickhouseHttp clickhouseHttp = new ClickhouseHttp("jdbc:clickhouse://localhost:9123");
    TestDataProvider.Data testData;

    final ThreadFactory threadFactory = ThreadFactories.simpleDaemonFactory();
    final Statement delete, create;
    {
        try {
            delete = Clickhouse.it().getConnection().createStatement();
            create = Clickhouse.it().getConnection().createStatement();
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

    @Setup(Level.Trial)
    public void setup() {
        Clickhouse.setApacheHttpClientLoggingSettings();
    }

    @Setup(Level.Iteration)
    public void setupIteration() {
        testData = TestDataProvider.R_1M__S_1__SQL_DATE_1__I_4__DOUBLE_4__E_0;
        deleteTable();
    }

    @BenchmarkMode(Mode.All)
    @OperationsPerInvocation(1_000_000)
    @Measurement(iterations = 5)
    @Warmup(iterations = 5)
    @org.openjdk.jmh.annotations.Benchmark
    @Threads(1)
    public void loadInOneThread(FromCreateToLoadBenchmark fromCreateToLoadBenchmark) {
        ClickhouseInsertBatch insertBatch = new ClickhouseInsertBatch(fromCreateToLoadBenchmark.clickhouseHttp,
                "INSERT INTO test.TEST_DATA_1M_9C_DATE (ID, f1, f2, f3, f4, f5, f6, f7, f8)",
                BATCH_SIZE,
                new BufferPreparedStream(BATCH_SIZE * 1000)
        );

        LineEventDisruptor parseCsvDisruptor = new LineEventDisruptor();
        Disruptor<LineEvent> lineEventDisruptor =
                parseCsvDisruptor.create(fromCreateToLoadBenchmark.threadFactory,
                        fromCreateToLoadBenchmark.testData.columnTypes,
                        new ChLineEventHandler(insertBatch));

        UnivosityReaderFromQuotedCsv reader = new UnivosityReaderFromQuotedCsv(',', '"',
                fromCreateToLoadBenchmark.testData.columnTypes,
                lineEventDisruptor.getRingBuffer());
        try {
            reader.readFile(fromCreateToLoadBenchmark.testData.file);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
        lineEventDisruptor.shutdown();
    }

    public void deleteTable() {
        try {
            delete.execute("drop table if exists test.TEST_DATA_1M_9C_DATE;");
            create.execute("create table if not exists test.TEST_DATA_1M_9C_DATE (ID Date, f1 Int32, f2 Int32, f3 Int32, f4 Int32, f5 Float64, f6 Float64, f7 Float64, f8 Float64) Engine = TinyLog;");
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

    public static void main(String[] args) throws Exception {
        new Runner(new OptionsBuilder()
                .include(FromCreateToLoadBenchmark.class.getSimpleName())
                .build()
        ).run();
    }

}

//        Benchmark                                                            Mode  Cnt       Score      Error  Units
//        FromCreateToLoadBenchmark.loadInOneThread                           thrpt   50  485619,407 ± 6114,060  ops/s
//        FromCreateToLoadBenchmark.loadInOneThread                            avgt   50      ≈ 10⁻⁶              s/op
//        FromCreateToLoadBenchmark.loadInOneThread                          sample   50      ≈ 10⁻⁶              s/op
//        FromCreateToLoadBenchmark.loadInOneThread:loadInOneThread·p0.00    sample           ≈ 10⁻⁶              s/op
//        FromCreateToLoadBenchmark.loadInOneThread:loadInOneThread·p0.50    sample           ≈ 10⁻⁶              s/op
//        FromCreateToLoadBenchmark.loadInOneThread:loadInOneThread·p0.90    sample           ≈ 10⁻⁶              s/op
//        FromCreateToLoadBenchmark.loadInOneThread:loadInOneThread·p0.95    sample           ≈ 10⁻⁶              s/op
//        FromCreateToLoadBenchmark.loadInOneThread:loadInOneThread·p0.99    sample           ≈ 10⁻⁶              s/op
//        FromCreateToLoadBenchmark.loadInOneThread:loadInOneThread·p0.999   sample           ≈ 10⁻⁶              s/op
//        FromCreateToLoadBenchmark.loadInOneThread:loadInOneThread·p0.9999  sample           ≈ 10⁻⁶              s/op
//        FromCreateToLoadBenchmark.loadInOneThread:loadInOneThread·p1.00    sample           ≈ 10⁻⁶              s/op
//        FromCreateToLoadBenchmark.loadInOneThread                              ss   50       2,069 ±    0,026   s/op