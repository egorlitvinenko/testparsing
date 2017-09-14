package org.egorlitvinenko.testflink;

/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.io.CsvInputFormat;
import org.apache.flink.api.java.io.TupleCsvInputFormat;
import org.apache.flink.api.java.io.jdbc.JDBCOutputFormat;
import org.apache.flink.api.java.tuple.Tuple9;
import org.apache.flink.api.java.typeutils.TupleTypeInfo;
import org.apache.flink.core.fs.Path;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.OutputFormatSinkFunction;
import org.apache.flink.types.Row;

import java.sql.Types;
import java.time.LocalDate;


/**
 * Skeleton for a Flink Streaming Job.
 * <p>
 * For a full example of a Flink Streaming Job, see the SocketTextStreamWordCount.java
 * file in the same package/directory or have a look at the website.
 * <p>
 * You can also generate a .jar file that you can submit on your Flink
 * cluster.
 * Just type
 * mvn clean package
 * in the projects root directory.
 * You will find the jar in
 * target/test-flink-1.0-SNAPSHOT.jar
 * From the CLI you can then run
 * ./bin/flink run -c StreamingJob target/test-flink-1.0-SNAPSHOT.jar
 * <p>
 * For more information on the CLI see:
 * <p>
 * http://flink.apache.org/docs/latest/apis/cli.html
 */
public class StreamingJob9C {

    public static void main(String[] args) throws Exception {
        // set up the streaming execution environment
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        final Path path = new Path("r1k__s1__d4__i4__errors_0_nq.csv");

        DataStream<Row> dataStream = env
                .createInput(createCsvInputFormat(path), TypeInformation.of(
                        new TypeHint<Tuple9<String, String, String, String, String, String, String, String, String>>() {
                        }))
                .map(new MapFunction<Tuple9<String, String, String, String, String, String, String, String, String>, Row>() {
                    @Override
                    public Row map(Tuple9<String, String, String, String, String, String, String, String, String> value) throws Exception {
                        return RowParser.parse(value).row;
                    }
                })
                .returns(
                        org.apache.flink.api.common.typeinfo.Types.ROW(
                                TypeInformation.of(LocalDate.class),
                                TypeInformation.of(Integer.class), TypeInformation.of(Integer.class),
                                TypeInformation.of(Integer.class), TypeInformation.of(Integer.class),
                                TypeInformation.of(Double.class), TypeInformation.of(Double.class),
                                TypeInformation.of(Double.class), TypeInformation.of(Double.class)
                        ));

        dataStream.addSink(
                new OutputFormatSinkFunction<>(
                        JDBCOutputFormat.buildJDBCOutputFormat()
                                .setDrivername("ru.yandex.clickhouse.ClickHouseDriver")
                                .setDBUrl("jdbc:clickhouse://localhost:9123")
                                .setQuery("INSERT INTO test.TEST_DATA_1M_9C_DATE (ID, f1, f2, f3, f4, f5, f6, f7, f8) values (?, ?, ?, ?, ?, ?, ?, ?, ?)")
                                .setSqlTypes(new int[]{Types.DATE,
                                        Types.INTEGER, Types.INTEGER, Types.INTEGER, Types.INTEGER,
                                        Types.DOUBLE, Types.DOUBLE, Types.DOUBLE, Types.DOUBLE})
                                .finish()
                )
        );
        // execute program
        env.execute("Flink Streaming Java API Skeleton");
    }

    static CsvInputFormat<Tuple9<String, String, String, String, String, String, String, String, String>> createCsvInputFormat(Path path) {
        TupleTypeInfo<Tuple9<String, String, String, String, String, String, String, String, String>> types =
                TupleTypeInfo.getBasicAndBasicValueTupleTypeInfo(
                        String.class, String.class, String.class,
                        String.class, String.class, String.class,
                        String.class, String.class, String.class);
        TupleCsvInputFormat<Tuple9<String, String, String, String, String, String, String, String, String>> format = new TupleCsvInputFormat(path, types);
        format.setCharset("UTF-8");
        format.setDelimiter(CsvInputFormat.DEFAULT_LINE_DELIMITER);
        format.setFieldDelimiter(",");
        format.setCommentPrefix(null);
        format.setLenient(false);
        format.setSkipFirstLineAsHeader(true);
        format.enableQuotedStringParsing('"');
        return format;
    }

}
