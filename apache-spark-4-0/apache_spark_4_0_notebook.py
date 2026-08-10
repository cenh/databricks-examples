# Databricks notebook source
# MAGIC %md
# MAGIC # What Developers Need to Know About Apache Spark 4.0
# MAGIC
# MAGIC **Article:** [What Developers Need to Know About Apache Spark 4.0](https://medium.com/@cralle/what-developers-need-to-know-about-apache-spark-4-0-508d0e4a5370?sk=2a635c3e28a7aa90c655d0a2da421725)
# MAGIC
# MAGIC **Author:** Christian Hansen (https://medium.com/@cralle)
# MAGIC
# MAGIC **Published:** Aug 26, 2025
# MAGIC
# MAGIC A tour of the Apache Spark 4.0 features that matter most for day to day data engineering at CH Enterprise: SQL-defined UDFs, parameter markers, collations, ANSI SQL mode by default, the new VARIANT data type, the Python Data Source API, and the streaming updates (state store improvements, transformWithState, and the State Reader API). Spark 4.0 shipped in Databricks Runtime 17.0/17.1, ahead of the LTS runtime.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Setup
# MAGIC
# MAGIC Creates the `testing.default` schema (if it does not already exist) and a set of small CH Enterprise sample tables used throughout this notebook: an orders table for the SQL UDF and parameter marker demos, an offices table for the collations demo, and a raw application log table for the VARIANT demo. The Python Data Source API and streaming sections generate their own sample data further down.

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE CATALOG IF NOT EXISTS testing;
# MAGIC CREATE SCHEMA IF NOT EXISTS testing.default;
# MAGIC USE CATALOG testing;
# MAGIC USE SCHEMA default;

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE testing.default.ch_enterprise_orders (
# MAGIC   order_id INT,
# MAGIC   customer_name STRING,
# MAGIC   region STRING,
# MAGIC   amount_usd DOUBLE
# MAGIC );
# MAGIC
# MAGIC INSERT INTO testing.default.ch_enterprise_orders VALUES
# MAGIC   (1001, 'Nordic Retail A/S', 'EMEA', 4820.50),
# MAGIC   (1002, 'Baltic Freight OY', 'EMEA', 990.00),
# MAGIC   (1003, 'Harbor Logistics Inc', 'AMER', 15200.75);

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE testing.default.ch_enterprise_offices (
# MAGIC   office_name STRING COLLATE da,
# MAGIC   office_name_en STRING COLLATE UTF8_LCASE
# MAGIC );
# MAGIC
# MAGIC INSERT INTO testing.default.ch_enterprise_offices VALUES
# MAGIC   ('København', 'Copenhagen'),
# MAGIC   ('Århus', 'Aarhus'),
# MAGIC   ('Roskilde', 'Roskilde');

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE testing.default.ch_enterprise_app_logs_raw (
# MAGIC   log_id INT,
# MAGIC   raw_json STRING
# MAGIC );
# MAGIC
# MAGIC INSERT INTO testing.default.ch_enterprise_app_logs_raw VALUES
# MAGIC   (1, '{"service": "orders-service", "level": "INFO", "latency_ms": 42, "order_id": "ORD-1001"}'),
# MAGIC   (2, '{"service": "auth-service", "level": "WARN", "user_id": "u-582", "attempt": 3}'),
# MAGIC   (3, '{"service": "orders-service", "level": "ERROR", "latency_ms": 980, "order_id": "ORD-1002", "error": "timeout"}');

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. SQL-Defined UDFs
# MAGIC
# MAGIC Spark 4.0 lets you define a user-defined function directly in pure SQL, with no Python or Scala involved. It is registered in Unity Catalog like any other function, so it can be governed and reused across notebooks and jobs.

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE FUNCTION testing.default.ch_left_pad(
# MAGIC   x INT COMMENT 'Any number',
# MAGIC   pad INT COMMENT 'Target width'
# MAGIC )
# MAGIC RETURNS STRING
# MAGIC COMMENT 'Left-pads a number with zeros to a fixed width'
# MAGIC CONTAINS SQL
# MAGIC DETERMINISTIC
# MAGIC RETURN lpad(x, pad, '0');

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT order_id, testing.default.ch_left_pad(order_id, 6) AS padded_order_id
# MAGIC FROM testing.default.ch_enterprise_orders
# MAGIC ORDER BY order_id;

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Parameter Markers
# MAGIC
# MAGIC Parameter markers let you pass external values into a SQL string safely, instead of concatenating them into the query text. Spark 4.0 supports both named markers (`:name`) and unnamed markers (`?`), used together with `DECLARE` and `EXECUTE IMMEDIATE`. This reduces the risk of SQL injection when building queries dynamically, for example from an application layer at CH Enterprise.

# COMMAND ----------

# MAGIC %sql
# MAGIC DECLARE ch_order_lookup_named = 'SELECT :order_id AS order_id, :qty AS quantity, :qty * 12 AS annualized_quantity';
# MAGIC EXECUTE IMMEDIATE ch_order_lookup_named USING 1001 AS order_id, 35 AS qty;

# COMMAND ----------

# MAGIC %sql
# MAGIC DECLARE ch_order_lookup_unnamed = 'SELECT ? AS order_id, ? AS quantity';
# MAGIC EXECUTE IMMEDIATE ch_order_lookup_unnamed USING 1001, 35;

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Collations
# MAGIC
# MAGIC A `COLLATE` clause on a `STRING` column controls how comparisons and sorting behave: language and region-aware ordering, case sensitivity, accent sensitivity, and trailing-blank sensitivity. The CH Enterprise offices table below stores each office name once in a Danish-aware collation (`da`) and once in a case-insensitive collation (`UTF8_LCASE`), and the two queries show the difference.
# MAGIC
# MAGIC **Caveat:** collation support depends on the runtime; use Databricks Runtime 17.0/17.1 or later.

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM testing.default.ch_enterprise_offices WHERE office_name = 'København';

# COMMAND ----------

# MAGIC %sql
# MAGIC -- UTF8_LCASE is case-insensitive, so a lowercase lookup still matches "Copenhagen"
# MAGIC SELECT * FROM testing.default.ch_enterprise_offices WHERE office_name_en = 'copenhagen';

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. ANSI SQL Mode by Default
# MAGIC
# MAGIC Spark 4.0 turns on ANSI SQL mode by default. Arithmetic overflow now raises an error instead of silently wrapping around, which makes behavior more predictable and closer to other SQL engines. To keep this notebook runnable end to end, the cell below demonstrates the difference by toggling `spark.sql.ansi.enabled` explicitly and by using `try_add`, which returns `NULL` on overflow instead of raising, rather than triggering an unhandled exception.

# COMMAND ----------

# MAGIC %sql
# MAGIC -- With ANSI mode off, integer overflow wraps around silently (legacy Spark behavior)
# MAGIC SET spark.sql.ansi.enabled = false;
# MAGIC SELECT 2147483647 + 1 AS overflow_wraps_when_ansi_disabled;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- With ANSI mode on (the Spark 4.0 default), the same expression would raise an
# MAGIC -- ArithmeticException. try_add() shows the safe way to handle that case: it
# MAGIC -- returns NULL on overflow instead of raising, so this cell still runs cleanly.
# MAGIC SET spark.sql.ansi.enabled = true;
# MAGIC SELECT try_add(2147483647, 1) AS overflow_returns_null_with_try_add;

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5. Introducing the VARIANT Data Type
# MAGIC
# MAGIC `VARIANT` stores semi-structured data (typically JSON) without forcing it into a rigid schema up front, while still being far more efficient to query than a raw JSON string column. It is a good fit for data like the CH Enterprise application logs below, where different services emit different fields in the same log stream.

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE testing.default.ch_enterprise_app_logs_variant AS
# MAGIC SELECT log_id, PARSE_JSON(raw_json) AS payload
# MAGIC FROM testing.default.ch_enterprise_app_logs_raw;
# MAGIC
# MAGIC SELECT log_id, payload:service, payload:level, payload:latency_ms
# MAGIC FROM testing.default.ch_enterprise_app_logs_variant
# MAGIC ORDER BY log_id;

# COMMAND ----------

import pyspark.sql.functions as F

variant_df = spark.table("testing.default.ch_enterprise_app_logs_variant")

# variant_get() pulls a typed value out of the VARIANT column by path;
# schema_of_variant() reports the shape actually observed for that row.
variant_df.select(
    "log_id",
    F.variant_get(F.col("payload"), "$.service", "string").alias("service"),
    F.variant_get(F.col("payload"), "$.latency_ms", "int").alias("latency_ms"),
    F.schema_of_variant(F.col("payload")).alias("inferred_schema"),
).orderBy("log_id").display()

# COMMAND ----------

# MAGIC %md
# MAGIC ## 6. Python Data Source API
# MAGIC
# MAGIC The Python Data Source API lets Spark treat a custom source as if it were built in, without a third-party connector. This is a small self-contained example modeled on the article's IoT/GoogleSheets style example: a `ch_enterprise_iot` source that generates synthetic sensor readings, defined with `schema()` and `reader()` and registered for the current session.

# COMMAND ----------

import random
from datetime import datetime, timedelta

from pyspark.sql.datasource import DataSource, DataSourceReader
from pyspark.sql.types import DoubleType, StringType, StructField, StructType, TimestampType


class ChEnterpriseIotDataSource(DataSource):
    """A synthetic IoT sensor reading source for CH Enterprise demos."""

    @classmethod
    def name(cls):
        return "ch_enterprise_iot"

    def schema(self):
        return StructType(
            [
                StructField("sensor_id", StringType()),
                StructField("reading_ts", TimestampType()),
                StructField("temperature_c", DoubleType()),
            ]
        )

    def reader(self, schema: StructType):
        return ChEnterpriseIotReader(self.options)


class ChEnterpriseIotReader(DataSourceReader):
    def __init__(self, options):
        self.num_rows = int(options.get("num_rows", 10))

    def read(self, partition):
        base_time = datetime(2026, 8, 1)
        rng = random.Random(42)
        for i in range(self.num_rows):
            yield (
                f"sensor-{i % 3}",
                base_time + timedelta(minutes=i),
                round(rng.uniform(18.0, 32.0), 2),
            )


spark.dataSource.register(ChEnterpriseIotDataSource)

iot_df = spark.read.format("ch_enterprise_iot").option("num_rows", 12).load()
iot_df.display()

# COMMAND ----------

# MAGIC %md
# MAGIC ## 7. Streaming: State Store Enhancements
# MAGIC
# MAGIC Spark 4.0 reworks the internal state checkpoint format, improves SST file reuse and snapshot maintenance, and adds clearer error classification for state store failures. These are internal engine improvements with no new user-facing API, so there is no separate runnable demo for this item; it benefits the `transformWithState` and State Reader API demos below automatically.

# COMMAND ----------

# MAGIC %md
# MAGIC ## 8. Streaming: transformWithState
# MAGIC
# MAGIC `transformWithState` (available in Scala, Java, and Python as `transformWithStateInPandas`) adds arbitrary, object-oriented stateful processing to Structured Streaming: composite state types, schema evolution, timers, native TTL, and initial state handling. The demo below runs a small stateful query over a deterministic Delta source of synthetic CH Enterprise sensor readings, grouping by `sensor_id` and keeping a running count per sensor in state. It uses an `availableNow` trigger so the query processes the fixed set of rows once and then stops, which keeps the notebook runnable end to end on serverless compute.
# MAGIC
# MAGIC **Caveat:** `transformWithStateInPandas` requires Databricks Runtime 17.0/17.1 or later (Spark 4.0), and the exact class and method names are new in this release, so check the docs for your runtime version before relying on this in production.

# COMMAND ----------

from typing import Iterator

import pandas as pd
import pyspark.sql.functions as F
from pyspark.sql.streaming import StatefulProcessor, StatefulProcessorHandle
from pyspark.sql.types import LongType, StringType, StructField, StructType


class ChEnterpriseSensorCountProcessor(StatefulProcessor):
    """Keeps a running reading count per sensor_id using the transformWithState state API."""

    def init(self, handle: StatefulProcessorHandle) -> None:
        state_schema = StructType([StructField("count", LongType(), True)])
        self.count_state = handle.getValueState("count_state", state_schema)

    def handleInputRows(
        self, key, rows: Iterator[pd.DataFrame], timer_values
    ) -> Iterator[pd.DataFrame]:
        existing = self.count_state.get()
        count = existing[0] if existing is not None else 0
        for pdf in rows:
            count += len(pdf)
        self.count_state.update((count,))
        yield pd.DataFrame({"sensor_id": [key[0]], "reading_count": [count]})

    def close(self) -> None:
        pass


output_schema = StructType(
    [
        StructField("sensor_id", StringType(), True),
        StructField("reading_count", LongType(), True),
    ]
)

# Deterministic Delta source: write a fixed batch of synthetic sensor readings, then
# stream-read it with an availableNow trigger so the query processes exactly these rows
# once and stops. This is both reproducible and compatible with serverless compute
# (which does not support the continuous ProcessingTime trigger).
source_table = "testing.default.ch_enterprise_sensor_readings"
spark.sql(f"DROP TABLE IF EXISTS {source_table}")
(
    spark.range(30)
    .withColumn("sensor_id", F.concat(F.lit("sensor-"), (F.col("id") % 3).cast("string")))
    .select("sensor_id")
    .write.format("delta")
    .mode("overwrite")
    .saveAsTable(source_table)
)

checkpoint_path = "/tmp/testing/apache_spark_4_0/transform_with_state_checkpoint"
dbutils.fs.rm(checkpoint_path, True)

ch_sensor_stream = spark.readStream.table(source_table)

ch_sensor_query = (
    ch_sensor_stream.groupBy("sensor_id")
    .transformWithStateInPandas(
        statefulProcessor=ChEnterpriseSensorCountProcessor(),
        outputStructType=output_schema,
        outputMode="Update",
        timeMode="None",
    )
    .writeStream.format("memory")
    .queryName("ch_enterprise_sensor_counts")
    .outputMode("update")
    .option("checkpointLocation", checkpoint_path)
    .trigger(availableNow=True)
    .start()
)

ch_sensor_query.awaitTermination()

spark.sql("SELECT * FROM ch_enterprise_sensor_counts ORDER BY reading_count DESC").display()

# COMMAND ----------

# MAGIC %md
# MAGIC ## 9. Streaming: State Reader API
# MAGIC
# MAGIC The State Reader API exposes a streaming query's internal state as a readable batch DataFrame, using the same checkpoint location the query wrote to. It supports two formats: `state-metadata` for a high-level view of operators, batch IDs, and partitioning, and `statestore` for the granular key-value contents. This reuses the checkpoint from the `transformWithState` query above, since the query has already been stopped.

# COMMAND ----------

state_metadata_df = spark.read.format("state-metadata").load(checkpoint_path)
state_metadata_df.display()

# COMMAND ----------

# The statestore reader requires the name of the state variable to read; this is the
# same name passed to getValueState() in the StatefulProcessor above ("count_state").
state_store_df = (
    spark.read.format("statestore")
    .option("stateVarName", "count_state")
    .load(checkpoint_path)
)
state_store_df.display()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Cleanup
# MAGIC
# MAGIC Drops the tables and function created during Setup, drops the in-memory streaming temp view, and removes the temporary checkpoint directory used by the streaming demos.

# COMMAND ----------

# MAGIC %sql
# MAGIC DROP TABLE IF EXISTS testing.default.ch_enterprise_sensor_readings;
# MAGIC DROP TABLE IF EXISTS testing.default.ch_enterprise_app_logs_variant;
# MAGIC DROP TABLE IF EXISTS testing.default.ch_enterprise_app_logs_raw;
# MAGIC DROP TABLE IF EXISTS testing.default.ch_enterprise_offices;
# MAGIC DROP TABLE IF EXISTS testing.default.ch_enterprise_orders;
# MAGIC DROP FUNCTION IF EXISTS testing.default.ch_left_pad;

# COMMAND ----------

spark.catalog.dropTempView("ch_enterprise_sensor_counts")
dbutils.fs.rm(checkpoint_path, True)

# COMMAND ----------

# MAGIC %md
# MAGIC **Notes:**
# MAGIC - This article was published Aug 26, 2025, when Spark 4.0 was available in Databricks Runtime 17.0/17.1; the LTS runtime for DBR 17 arrived later (see the article's Oct 2025 edit note). Prefer the LTS runtime once you can for anything you plan to keep running long term.
# MAGIC - `transformWithStateInPandas`, the State Reader API formats (`state-metadata`, `statestore`), and the VARIANT functions (`variant_get`, `schema_of_variant`) are new in Spark 4.0 / DBR 17+; verify exact signatures against the docs for your runtime, since APIs introduced in a `.0` release can still see minor changes.
# MAGIC - The streaming demos use a small Delta source table, an `availableNow` trigger, and a `memory` sink purely to keep this notebook self-contained, deterministic, and runnable end to end (including on serverless, which does not support the continuous `ProcessingTime` trigger). Point `transformWithStateInPandas` at a real streaming source/sink and a durable checkpoint location for production use.
