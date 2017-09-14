# testparsing

It is only tests.
No guaranties.
Code could be not accurate, duplicated, so it is not an example "how to do".

Two scenarios:

1. Read 5 columns, 1 M rows, all strings and write to CH;
1. Read 9 columns, 1 M rows, parse 1 date, 4 integer, 4 double, write to CH;

Last results:
* Flink (StreamingJob, 5 string columns, 1M rows to Clickhouse, parallelism 1 ~ 5 seconds, parallelism 2 - 3 seconds) [1];
* Flink (<a href="https://github.com/egorlitvinenko/testparsing/blob/master/test-flink/src/main/java/org/egorlitvinenko/testflink/BatchJob9C.java">BatchJob9C</a> - created after consultancy with developers, I don't remember result, but it is not impressive or good (expectedly in this case)) [1];
* Spark (SimpleReadCsvAndWriteToClickhouse ~20 seconds (when writing to CH), 1 date, 4 double, 4 integer, 1M rows) [1];
* Disruptor implementation (UnivosityParsingBatchMain, ~200 000 rows per second, 1 date, 4 double, 4 integer, 1M rows, TableRowWithArrays) [1];
* Disruptor implementation (UnivosityParsingPacketMain2, ~200000+ rows per second, 1 date, 4 double, 4 integer, 1M rows, Event implements TableRow) [1];
* Univosity Only (UnivosityBaselineMain, ~ 83500 rows per second, 1 date, 4 double, 4 integer, 1M rows) [1];
* Univosity + ~~Parsing~~ -> Validation + <a href="https://github.com/egorlitvinenko/clickhouse-input-client">http streaming</a>: 480k+ thrpt,  1 µs sample per operation (<a href="https://github.com/egorlitvinenko/testparsing/blob/master/testdisruptor/src/main/java/org/egorlitvinenko/testdisruptor/clickhousestream/FromCreateToLoadBenchmark.java">benchmark</a>) [1];

The results are not final. All implementations could/should be improved.

Questions:

1. StreamingJob9c doesn't work. (No exceptions)
2. How to implement built-in Univosity parsing.

Test configuration

[1] Ubuntu 14.04, 3.19.0-66-generic, Clickhouse 1.1.54189, x64, Little Endian, 4 CPU, L1d cache: 32K, L1i cache: 32K, L2 cache: 256K, L3 cache: 3072K, Intel(R) Core(TM) i5-3230M CPU @ 2.60GHz, 16 GB DDR3 RAM; Samsung SSD 850 EVO 250GB (EMT01B6Q)
