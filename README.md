# testparsing

It is only tests.
No guaranties.
Code could be not accurate, duplicated, so it is not an example "how to do".

Last results:
* Flink (StreamingJob, 5 string columns, 1M rows to Clickhouse, parallelism 1 ~ 5 seconds, parallelism 2 - 3 seconds);
* Spark (SimpleReadCsvAndWriteToClickhouse ~20 seconds (when writing to CH), 1 date, 4 double, 4 integer, 1M rows);
* Disruptor implementation (UnivosityParsingBatchMain, ~200 000 rows per second, 1 date, 4 double, 4 integer, 1M rows);
* Univosity Only (UnivosityBaselineMain, ~ 83500 rows per second, 1 date, 4 double, 4 integer, 1M rows)

The results are not final. All implementations could/should be improved.