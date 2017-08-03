package org.egorlitvinenko.testflink;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.io.jdbc.JDBCOutputFormat;
import org.apache.flink.api.java.tuple.Tuple5;
import org.apache.flink.core.fs.Path;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.OutputFormatSinkFunction;
import org.apache.flink.types.Row;

import java.sql.Types;

/**
 * @author Egor Litvinenko
 */
public class StreamingJobStringsBaseLine {


    public static void main(String[] args) throws Exception {
        // set up the streaming execution environment
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        final Path path = new Path("test-data.csv");

        DataStream<Row> dataStream = env
                .createInput(StreamingJob.createCsvInputFormat(path),
                        TypeInformation.of(new TypeHint<Tuple5<String, String, String, String, String>>() {} ))
                .map(new MapFunction<Tuple5<String, String, String, String, String>, Row>() {
                    @Override
                    public Row map(Tuple5<String, String, String, String, String> value) throws Exception {
                        return Row.of(value.getField(0),
                                value.getField(1),
                                value.getField(2),
                                value.getField(3),
                                value.getField(4));
                    }
                });

        dataStream.addSink(
                new OutputFormatSinkFunction<>(
                        JDBCOutputFormat.buildJDBCOutputFormat()
                                .setDrivername("ru.yandex.clickhouse.ClickHouseDriver")
                                .setDBUrl("jdbc:clickhouse://localhost:9123")
                                .setQuery("INSERT INTO test.TEST_DATA_1M_STRING (ID, f1, f2, f3, f4) values (?, ?, ?, ?, ?)")
                                .setSqlTypes(new int[] {Types.VARCHAR, Types.VARCHAR, Types.VARCHAR, Types.VARCHAR, Types.VARCHAR})
                                .finish()
                )
        );
        // execute program
        env.execute("Flink Streaming Java API Skeleton (Either)");
    }

}
