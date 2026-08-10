**Article:** [What Developers Need to Know About Apache Spark 4.1](https://medium.com/@cralle/what-developers-need-to-know-about-apache-spark-4-1-e013ccd838f8?sk=d8b6accb0402bc0c601931d677774de2)

# What Developers Need to Know About Apache Spark 4.1

A runnable tour of the headline Apache Spark 4.1 features for developers: Structured Streaming Real-Time Mode, Spark Declarative Pipelines, faster PySpark with Arrow UDFs and UDTFs, the Python Data Source API's new filter pushdown, Spark Connect and Spark ML GA, and the new SQL capabilities (SQL Scripting, VARIANT, recursive CTEs, and approximate data sketches). Spark 4.1 is available today in Databricks Runtime 18.0 (currently in Beta).

The notebook creates a small set of CH Enterprise sample tables (orders, customers, an employee hierarchy, and product ratings), then works through one numbered section per feature with a small demo against that data, and cleans everything up at the end. A few cells (Real-Time Mode's trigger, Spark Declarative Pipelines, and the newest sketch function names) are shown as reference code rather than executed, because they need infrastructure this notebook does not provision or syntax that is still settling between Spark 4.1 point releases; each is called out inline.

## Files

- `apache_spark_4_1_notebook.py` - Databricks notebook (Python/SQL) covering all ten features: setup, Real-Time Mode, Spark Declarative Pipelines, Arrow UDFs/UDTFs, Python worker logging, Python Data Source filter pushdown, Spark Connect/Spark ML GA, SQL Scripting, VARIANT, recursive CTEs, approximate data sketches, and cleanup.

## Requirements

- Databricks Runtime 18.0 (Beta at time of writing), the first runtime to ship Apache Spark 4.1
- Unity Catalog enabled workspace, with `CREATE TABLE` privilege on `testing.default`
- Serverless compute is sufficient: the notebook was validated end to end on serverless, using a serverless environment version that ships the Apache Spark 4.1 / PySpark 4.0 client (older serverless environment versions ship PySpark 3.5 and do not expose the 4.1 APIs). A handful of cells rely on configs or APIs that are only available on a classic Databricks Runtime 18.0 cluster and are shown as commented-out reference code instead (see Manual setup required below)

## Setup

Run the Setup cells first. They create four managed Delta tables under `testing.default`:

- `ch_customers` - CH Enterprise customer records
- `ch_orders` - CH Enterprise order records, used by the streaming, Arrow, and sketch demos
- `ch_employees` - a manager hierarchy, used by the recursive CTE demo
- `ch_product_ratings` - a small ratings table, used by the Spark ML on Connect demo

The VARIANT demo (section 8) creates a fifth table, `ch_events`, inline in its own cell.

## Manual setup required

None. Every feature in this article is a language, API, or SQL-level change rather than a workspace configuration or infrastructure change, so the notebook runs end to end (validated on serverless) without any manual setup outside of having a workspace on Databricks Runtime 18.0. Some cells are shown as commented-out reference code instead of being forced to run, either because they need infrastructure this notebook does not provision (a live Kafka broker for Real-Time Mode, a Declarative Pipeline job for Spark Declarative Pipelines) or because they depend on a config or API that is not available on serverless compute:

- Real-Time Mode's `spark.databricks.streaming.realTimeMode.enabled` and Python worker logging's `spark.sql.pyspark.worker.logging.enabled` cannot be set on serverless, and `spark.tvf.python_worker_logs()` is not exposed there.
- The Python Data Source filter pushdown flag `spark.sql.pysparkDataSourceFilterPushdown.enabled` cannot be enabled on serverless, so section 5 runs a custom Python data source without `pushFilters` and shows the pushdown version as reference.
- `ALSModel.recommendForAllUsers` uses Spark higher-order functions, which are unsupported on Unity Catalog serverless compute; section 6 trains the model and scores it with `transform`, and shows `recommendForAllUsers` as reference.

To run these reference cells, use a classic Databricks Runtime 18.0 cluster.

## Cleanup

Run the Cleanup cell at the end of the notebook to drop all five sample tables (`ch_customers`, `ch_orders`, `ch_employees`, `ch_product_ratings`, `ch_events`) from `testing.default`.
