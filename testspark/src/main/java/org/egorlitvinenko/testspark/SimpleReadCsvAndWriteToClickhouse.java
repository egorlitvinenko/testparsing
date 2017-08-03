package org.egorlitvinenko.testspark;

import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataTypes;

import java.util.Arrays;

/**
 * @author Egor Litvinenko
 */
public class SimpleReadCsvAndWriteToClickhouse {

    static SparkSession sparkSession = null;

    public static void main(String[] args) {
        sparkSession = SparkSession
                .builder()
                .appName("Spark Demo")
                .master("spark://egor-Lenovo-IdeaPad-Z500:7077")
                .getOrCreate();

        long parsedRows = sparkSession.read()
                .format("com.databricks.spark.csv")
                .option("path", "r1m__s1__d4__i4__errors_0.csv")
                .option("header", "true")
                .option("parserLib", "univocity")
                .option("dateFormat", "yyyy-MM-dd")
                .option("quote", "\"")
                .option("delimiter", ",")
                .schema(DataTypes.createStructType(
                        Arrays.asList(
                                DataTypes.createStructField("Date", DataTypes.DateType, false),
                                DataTypes.createStructField("f1", DataTypes.IntegerType, false),
                                DataTypes.createStructField("f2", DataTypes.IntegerType, false),
                                DataTypes.createStructField("f3", DataTypes.IntegerType, false),
                                DataTypes.createStructField("f4", DataTypes.IntegerType, false),
                                DataTypes.createStructField("f5", DataTypes.DoubleType, false),
                                DataTypes.createStructField("f6", DataTypes.DoubleType, false),
                                DataTypes.createStructField("f7", DataTypes.DoubleType, false),
                                DataTypes.createStructField("f8", DataTypes.DoubleType, false)
                        )
                ))
                .load().count();
        System.out.println(parsedRows);
        // 2 - 3 seconds

    }

}
