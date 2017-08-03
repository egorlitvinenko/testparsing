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

import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.io.jdbc.JDBCOutputFormat;
import org.apache.flink.api.java.tuple.Tuple5;
import org.apache.flink.types.Either;
import org.apache.flink.types.Row;
import org.apache.flink.util.Collector;

import java.sql.Types;

/**
 * Skeleton for a Flink Batch Job.
 * <p>
 * For a full example of a Flink Batch Job, see the WordCountJob.java file in the
 * same package/directory or have a look at the website.
 * <p>
 * You can also generate a .jar file that you can submit on your Flink
 * cluster.
 * Just type
 * mvn clean package
 * in the projects root directory.
 * You will find the jar in
 * target/test-flink-1.0-SNAPSHOT.jar
 * From the CLI you can then run
 * ./bin/flink run -c BatchJob target/test-flink-1.0-SNAPSHOT.jar
 * <p>
 * For more information on the CLI see:
 * <p>
 * http://flink.apache.org/docs/latest/apis/cli.html
 */
public class BatchJobEither {

    public static void main(String[] args) throws Exception {
        // set up the batch execution environment
        final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

        DataSet<Either<Row, Row>> dataSet = env
                .readCsvFile("test-data.csv")
                .fieldDelimiter(",")
                .parseQuotedStrings('"')
                .ignoreFirstLine()
                .types(String.class, String.class, String.class, String.class, String.class)
                .map(new MapFunction<Tuple5<String, String, String, String, String>, Either<Row, Row>>() {
                    @Override
                    public Either<Row, Row> map(Tuple5<String, String, String, String, String> value) throws Exception {
                        return RowParser.parse2(value).result;
                    }
                })
                .returns(new TypeHint<Either<Row, Row>>() {
                });


        DataSet<Row> bad = dataSet
                .filter(new FilterFunction<Either<Row, Row>>() {
                    @Override
                    public boolean filter(Either<Row, Row> value) throws Exception {
                        return value.isLeft();
                    }
                })
                .flatMap(new FlatMapFunction<Either<Row, Row>, Row>() {
                    @Override
                    public void flatMap(Either<Row, Row> value, Collector<Row> out) throws Exception {
                        out.collect(value.left());
                    }
                })
                .returns(Row.class);

        DataSet<Row> good = dataSet
                .filter(new FilterFunction<Either<Row, Row>>() {
                    @Override
                    public boolean filter(Either<Row, Row> value) throws Exception {
                        return value.isRight();
                    }
                })
                .flatMap(new FlatMapFunction<Either<Row, Row>, Row>() {
                    @Override
                    public void flatMap(Either<Row, Row> value, Collector<Row> out) throws Exception {
                        out.collect(value.right());
                    }
                })
                .returns(Row.class);

        bad.output(
                JDBCOutputFormat.buildJDBCOutputFormat()
                        .setDrivername("ru.yandex.clickhouse.ClickHouseDriver")
                        .setDBUrl("jdbc:clickhouse://localhost:9123")
                        .setQuery("INSERT INTO test.TEST_DATA_1M_STRING (ID, f1, f2, f3, f4) values (?, ?, ?, ?, ?)")
                        .setSqlTypes(new int[]{Types.VARCHAR, Types.VARCHAR, Types.VARCHAR, Types.VARCHAR, Types.VARCHAR})
                        .finish()
        );
        good.output(
                JDBCOutputFormat.buildJDBCOutputFormat()
                        .setDrivername("ru.yandex.clickhouse.ClickHouseDriver")
                        .setDBUrl("jdbc:clickhouse://localhost:9123")
                        .setQuery("INSERT INTO test.TEST_DATA_1M (ID, f1, f2, f3, f4) values (?, ?, ?, ?, ?)")
                        .setSqlTypes(new int[]{Types.VARCHAR, Types.DOUBLE, Types.DOUBLE, Types.DOUBLE, Types.DOUBLE})
                        .finish()
        );

        /**
         * Here, you can start creating your execution plan for Flink.
         *
         * Start with getting some data from the environment, like
         * 	env.readTextFile(textPath);
         *
         * then, transform the resulting DataSet<String> using operations
         * like
         * 	.filter()
         * 	.flatMap()
         * 	.join()
         * 	.coGroup()
         *
         * and many more.
         * Have a look at the programming guide for the Java API:
         *
         * http://flink.apache.org/docs/latest/apis/batch/index.html
         *
         * and the examples
         *
         * http://flink.apache.org/docs/latest/apis/batch/examples.html
         *
         */

        // execute program
        env.execute("Flink Batch Java API Skeleton");
    }
}
