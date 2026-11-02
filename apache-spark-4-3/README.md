**Article:** [Apache Spark 4.3: What Developers Need to Know](https://medium.com/@cralle/1896a29f1538?sk=9da546846112e07966435aa60a289207)

# Apache Spark 4.3: What Developers Need to Know

Apache Spark 4.3.0 (released October 30, 2026) is the first feature release on Spark's new quarterly schedule. This notebook runs the features the article covers, using a sample company called CH Enterprise: `ASOF JOIN` for point-in-time joins, Python UDFs transpiled into native Catalyst expressions, `JSON_TABLE` and `UNNEST ... WITH ORDINALITY`, the new `VARIANT` edit functions, nanosecond timestamps, `counter_diff`, `jaro_winkler_similarity`, `hmac`, and `DataFrame.zip`. It also shows the new SCD Type 2 Auto CDC flow for Spark Declarative Pipelines as reference code.

Release status: everything here ships in Apache Spark 4.3.0. `ASOF JOIN` and nanosecond timestamps are previews that ship disabled, and Python UDF transpilation is experimental; the notebook turns on each flag in the cell that needs it.

## Files

- `spark_4_3_developer_tour.py` - Databricks notebook (SQL + PySpark) covering every example in the article, in the same order, using temporary views for all sample data.

## Requirements

- Apache Spark 4.3.0. Locally: `pip install "pyspark[sql]==4.3.0"` with Java 17, 21, or 25 and Python 3.11 or later. At the time of writing, Databricks Runtime 19 ships Apache Spark 4.2, so on Databricks you need a runtime that ships Spark 4.3.
- pandas 2.2 or later, below 3.0, and PyArrow 18 or later (the PySpark 4.3 SQL dependencies).
- ANSI mode on (`spark.sql.ansi.enabled`, the default since Spark 4.0), which Python UDF transpilation requires.
- Session configs set by the notebook itself: `spark.sql.join.asofJoin.enabled`, `spark.sql.experimental.optimizer.transpilePyUDFs`, and `spark.sql.timestampNanosTypes.enabled`. It also pins `spark.sql.session.timeZone` to `UTC` so the timestamp outputs match the article.
- No tables, catalogs, or privileges are needed: all sample data lives in temporary views. `CATALOG` and `SCHEMA` at the top (`cenh_testing.default`) are only used if you choose to persist results.
- The Auto CDC section is reference code for a Spark Declarative Pipelines source file and does not run interactively.

## How to run

1. Import `spark_4_3_developer_tour.py` into a Databricks workspace attached to a Spark 4.3 runtime, or run it locally as a Python script with a `SparkSession` on PySpark 4.3.0.
2. Optionally adjust `CATALOG` and `SCHEMA` in the first cell.
3. Run the cells in order. The first cell asserts that the session is on Spark 4.3.
4. To try the Auto CDC example, copy the Python or SQL from the last section into a pipeline source file and run it with the `spark-pipelines` CLI against a CDC source of your own.
