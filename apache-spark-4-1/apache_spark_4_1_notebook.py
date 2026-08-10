# Databricks notebook source
# MAGIC %md
# MAGIC # What Developers Need to Know About Apache Spark 4.1
# MAGIC
# MAGIC **Author:** Christian Hansen ([https://medium.com/@cralle](https://medium.com/@cralle))
# MAGIC
# MAGIC **Published:** January 12, 2026
# MAGIC
# MAGIC A runnable tour of the Apache Spark 4.1 features that matter for developers: Structured Streaming Real-Time Mode, Spark Declarative Pipelines, faster PySpark with Arrow, the Python Data Source API, Spark Connect and Spark ML GA, and the new SQL capabilities (SQL Scripting, VARIANT, recursive CTEs, and approximate data sketches). Sample data uses the fictional company CH Enterprise and lives in `testing.default`.
# MAGIC
# MAGIC **Article:** [What Developers Need to Know About Apache Spark 4.1](https://medium.com/@cralle/what-developers-need-to-know-about-apache-spark-4-1-e013ccd838f8?sk=d8b6accb0402bc0c601931d677774de2)
# MAGIC
# MAGIC **Requires:** Databricks Runtime 18.0 (Beta at time of writing) for the Spark 4.1 specific features. Validated end to end on serverless compute. A few cells that need infrastructure this notebook does not provision (a live Kafka broker, a Declarative Pipeline job) or that depend on configs or APIs not available on serverless (Real-Time Mode, Python worker logging, Python data source filter pushdown, and `ALSModel.recommendForAllUsers`) are shown as commented-out reference code; see the closing notes for details.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Setup
# MAGIC
# MAGIC Creates the CH Enterprise sample tables used by every demo in this notebook: an orders and customers table, an employee hierarchy for the recursive CTE demo, and a small ratings table for the Spark ML on Connect demo. Everything is created in `testing.default` and dropped again in the Cleanup section at the end.

# COMMAND ----------

import pyspark.sql.functions as F
from pyspark.sql.types import StructType, StructField, IntegerType, StringType, DoubleType

catalog = "testing"
schema = "default"

spark.sql(f"USE CATALOG {catalog}")
spark.sql(f"USE SCHEMA {schema}")

# COMMAND ----------

customers_schema = StructType(
    [
        StructField("customer_id", IntegerType()),
        StructField("name", StringType()),
        StructField("region", StringType()),
        StructField("email", StringType()),
    ]
)
customers_data = [
    (1, "Nordvik AS", "EMEA", "nordvik@ch-enterprise.example"),
    (2, "Baltic Freight", "EMEA", "baltic@ch-enterprise.example"),
    (3, "Cascade Retail", "AMER", "cascade@ch-enterprise.example"),
    (4, "Sunrise Traders", "APAC", "sunrise@ch-enterprise.example"),
]
customers_df = spark.createDataFrame(customers_data, schema=customers_schema)
customers_df.write.mode("overwrite").saveAsTable(f"{catalog}.{schema}.ch_customers")

orders_schema = StructType(
    [
        StructField("order_id", IntegerType()),
        StructField("customer_id", IntegerType()),
        StructField("product", StringType()),
        StructField("quantity", IntegerType()),
        StructField("amount", DoubleType()),
        StructField("status", StringType()),
    ]
)
orders_data = [
    (1001, 1, "Pallet Wrap", 120, 899.50, "shipped"),
    (1002, 2, "Steel Coils", 5, 15200.00, "processing"),
    (1003, 3, "Retail Shelving", 40, 3120.75, "shipped"),
    (1004, 4, "Packaging Tape", 300, 410.00, "cancelled"),
    (1005, 1, "Pallet Wrap", 60, 449.75, "processing"),
]
orders_df = spark.createDataFrame(orders_data, schema=orders_schema)
orders_df.write.mode("overwrite").saveAsTable(f"{catalog}.{schema}.ch_orders")

employees_schema = StructType(
    [
        StructField("employee_id", IntegerType()),
        StructField("name", StringType()),
        StructField("manager_id", IntegerType()),
        StructField("department", StringType()),
    ]
)
employees_data = [
    (1, "Astrid Berg", None, "Executive"),
    (2, "Mikael Solberg", 1, "Engineering"),
    (3, "Priya Nair", 1, "Sales"),
    (4, "Tomas Krogh", 2, "Engineering"),
    (5, "Elin Dahl", 2, "Engineering"),
    (6, "Jonas Weber", 3, "Sales"),
]
employees_df = spark.createDataFrame(employees_data, schema=employees_schema)
employees_df.write.mode("overwrite").saveAsTable(f"{catalog}.{schema}.ch_employees")

ratings_schema = StructType(
    [
        StructField("user_id", IntegerType()),
        StructField("item_id", IntegerType()),
        StructField("rating", DoubleType()),
    ]
)
ratings_data = [
    (1, 101, 5.0),
    (1, 102, 3.0),
    (2, 101, 4.0),
    (2, 103, 2.0),
    (3, 102, 5.0),
    (3, 103, 4.0),
    (4, 101, 1.0),
    (4, 104, 5.0),
]
ratings_df = spark.createDataFrame(ratings_data, schema=ratings_schema)
ratings_df.write.mode("overwrite").saveAsTable(f"{catalog}.{schema}.ch_product_ratings")

print("Setup complete: ch_customers, ch_orders, ch_employees, ch_product_ratings created in testing.default")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Structured Streaming Real-Time Mode
# MAGIC
# MAGIC Spark 4.1 adds an official Real-Time Mode for Structured Streaming: a continuous execution path that keeps stages scheduled concurrently and moves data between them in memory, instead of the traditional discrete micro-batch model. The result is single-digit millisecond p99 latencies without rewriting existing Structured Streaming code, since the read/write API stays the same and only the trigger changes. Checkpointing still happens, on a configurable interval (default: 5 minutes).
# MAGIC
# MAGIC **Version note:** Real-Time Mode requires Databricks Runtime 18.0 (Beta) and is enabled per query with a configuration flag. On serverless compute the flag `spark.databricks.streaming.realTimeMode.enabled` cannot be set from a notebook cell (it raises `CONFIG_NOT_AVAILABLE`), so both the flag and the streaming query below are shown as reference code rather than executed.

# COMMAND ----------

# MAGIC %md
# MAGIC The cell below is shown as reference rather than executed: it needs a live Kafka broker, so it cannot run unattended in this notebook. It illustrates moving an existing CH Enterprise Kafka-to-Kafka job from micro-batch to continuous processing by changing only the `trigger(...)` call, with Real-Time Mode enabled first.

# COMMAND ----------

# Illustrative only - requires a live Kafka broker, not executed in this notebook.
# spark.conf.set("spark.databricks.streaming.realTimeMode.enabled", "true")
#
# broker_address = "<kafka-bootstrap-servers>"
# input_topic = "ch_orders_raw"
# output_topic = "ch_orders_enriched"
# checkpoint_location = "/Volumes/testing/default/checkpoints/ch_orders_realtime"
#
# streaming_query = (
#     spark.readStream.format("kafka")
#     .option("kafka.bootstrap.servers", broker_address)
#     .option("subscribe", input_topic)
#     .load()
#     .writeStream.format("kafka")
#     .option("kafka.bootstrap.servers", broker_address)
#     .option("topic", output_topic)
#     .option("checkpointLocation", checkpoint_location)
#     .outputMode("update")
#     .trigger(realTime="5 minutes")
#     .start()
# )

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Spark Declarative Pipelines (SDP)
# MAGIC
# MAGIC Spark Declarative Pipelines shift pipeline development from an imperative, step-by-step model to a declarative one: you declare tables and views with the `@dp.table` / `@dp.materialized_view` decorators, and Spark manages execution order, parallelism, fault tolerance, and retries. Key concepts are streaming tables, materialized views, temporary views, and pipeline graph execution.
# MAGIC
# MAGIC **Context note:** this code only runs inside an actual Declarative Pipeline job (a Lakeflow / Spark Declarative Pipeline), not in an interactive notebook cell, so per the code-notebooks rules it is shown as reference rather than executed here.

# COMMAND ----------

# Illustrative only - runs inside a Spark Declarative Pipeline, not interactively.
# %pip install pyspark[pipelines]
# from pyspark import pipelines as dp
# import pyspark.sql.functions as F
#
# @dp.table
# def ch_raw_orders():
#     return spark.readStream.format("kafka").option("subscribe", "ch_orders_raw").load()
#
# @dp.materialized_view
# def ch_customers_mv():
#     return spark.read.table("testing.default.ch_customers").filter(F.col("name").isNotNull())
#
# @dp.table
# def ch_fact_orders():
#     return (
#         spark.readStream.table("ch_raw_orders")
#         .filter(F.col("quantity") > 0)
#         .join(spark.table("ch_customers_mv"), on="customer_id", how="inner")
#     )

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Faster PySpark with Arrow UDFs and UDTFs
# MAGIC
# MAGIC Spark 4.1 adds `@arrow_udf` and `@arrow_udtf`, which operate on whole PyArrow arrays or record batches per call instead of one Python object at a time, cutting the serialization overhead between the JVM and the Python worker. The Arrow UDF below runs against the CH Enterprise orders table. The Arrow UDTF example is shown as reference code, since its exact invocation pattern (table-valued, batch-oriented) is still settling between Spark 4.1 point releases.

# COMMAND ----------

import pyarrow as pa
import pyarrow.compute as pc


@F.arrow_udf("int")
def product_name_length(values: pa.Array) -> pa.Array:
    return pc.utf8_length(values)


orders_df = spark.table(f"{catalog}.{schema}.ch_orders")
orders_df.select("product", product_name_length("product").alias("product_name_length")).show()

# COMMAND ----------

# MAGIC %md
# MAGIC Illustrative only, shown as reference rather than executed:

# COMMAND ----------

# @F.arrow_udtf(returnType="order_id: int, quantity: int, quantity_doubled: int")
# class QuantityDoubler:
#     def eval(self, batch: pa.RecordBatch):
#         order_id_col = batch.column("order_id")
#         quantity_col = batch.column("quantity")
#         doubled_col = pc.multiply(quantity_col, pa.scalar(2))
#         yield pa.RecordBatch.from_arrays(
#             [order_id_col, quantity_col, doubled_col],
#             names=["order_id", "quantity", "quantity_doubled"],
#         )

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Improved PySpark Debuggability: Python Worker Logging
# MAGIC
# MAGIC Spark 4.1 makes it possible to capture log statements emitted from inside a Python UDF and query them back as a Spark table, instead of digging through executor logs. Enable it with a Spark conf, log from the UDF with the standard `logging` module, and read the captured logs with the new `spark.tvf.python_worker_logs()` table-valued function.
# MAGIC
# MAGIC **Version note:** `spark.tvf.python_worker_logs()` is new in Spark 4.1 / Databricks Runtime 18.0. On earlier runtimes this call does not exist. On serverless compute the enabling flag `spark.sql.pyspark.worker.logging.enabled` cannot be set from a notebook cell (`CONFIG_NOT_AVAILABLE`) and the `python_worker_logs` table-valued function is not exposed, so the demo below is shown as reference code rather than executed; run it on a classic cluster on Databricks Runtime 18.0 to capture and query the worker logs.

# COMMAND ----------

# Illustrative only - the worker-logging flag and the python_worker_logs TVF are not
# available on serverless compute; run this on a classic Databricks Runtime 18.0 cluster.
# spark.conf.set("spark.sql.pyspark.worker.logging.enabled", "true")
#
#
# @F.udf("int")
# def log_order_id(order_id):
#     import logging
#
#     logging.getLogger("ch_enterprise_orders").warning(f"Handling order: {order_id}")
#     return order_id
#
#
# orders_df.select(log_order_id("order_id")).collect()
#
# # Read the captured logs back as a Spark table:
# spark.tvf.python_worker_logs().show(truncate=False)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5. Python Data Source API: Filter Pushdown
# MAGIC
# MAGIC The Python Data Source API (custom `DataSource` / `DataSourceReader` classes registered with `spark.dataSource.register`) gets filter pushdown in Spark 4.1: a reader can implement `pushFilters(self, filters)` to claim filters it can apply itself, and Spark removes those from the plan instead of re-applying them after the scan. The example below is a small in-memory CH Enterprise orders source that pushes down an equality filter on `status`.
# MAGIC
# MAGIC **Version note:** the Python Data Source API is available from Spark 4.0 onward; `pushFilters` is new in 4.1. Filter pushdown is gated by the config `spark.sql.pysparkDataSourceFilterPushdown.enabled`, which cannot be set on serverless compute (`CONFIG_NOT_AVAILABLE`). With pushdown disabled, a reader that implements `pushFilters` fails to initialize (`PYTHON_DATA_SOURCE_ERROR`), so the pushdown reader below is shown as reference code; run it on a classic Databricks Runtime 18.0 cluster with the flag enabled.
# MAGIC
# MAGIC A custom Python data source that does not implement `pushFilters` still works on serverless: Spark applies the `WHERE` filter itself after the scan. That non-pushdown version runs in the executable cell that follows.

# COMMAND ----------

# Illustrative only - filter pushdown requires spark.sql.pysparkDataSourceFilterPushdown.enabled,
# which is not settable on serverless; run this on a classic Databricks Runtime 18.0 cluster.
# from pyspark.sql.datasource import DataSource, DataSourceReader
#
#
# class ChOrdersPushdownSource(DataSource):
#     @classmethod
#     def name(cls):
#         return "ch_orders_pushdown_source"
#
#     def schema(self):
#         return "order_id int, customer_id int, product string, quantity int, amount double, status string"
#
#     def reader(self, schema):
#         return ChOrdersPushdownReader()
#
#
# class ChOrdersPushdownReader(DataSourceReader):
#     def __init__(self):
#         self._rows = [
#             (1001, 1, "Pallet Wrap", 120, 899.50, "shipped"),
#             (1002, 2, "Steel Coils", 5, 15200.00, "processing"),
#             (1003, 3, "Retail Shelving", 40, 3120.75, "shipped"),
#             (1004, 4, "Packaging Tape", 300, 410.00, "cancelled"),
#             (1005, 1, "Pallet Wrap", 60, 449.75, "processing"),
#         ]
#         self._status_filter_value = None
#
#     def pushFilters(self, filters):
#         remaining = []
#         for f in filters:
#             if f.__class__.__name__ == "EqualTo" and getattr(f, "attribute", None) == "status":
#                 self._status_filter_value = getattr(f, "value", None)
#             else:
#                 remaining.append(f)
#         return remaining
#
#     def read(self, partition):
#         for row in self._rows:
#             if self._status_filter_value is not None and row[5] != self._status_filter_value:
#                 continue
#             yield row
#
#
# spark.dataSource.register(ChOrdersPushdownSource)
# spark.read.format("ch_orders_pushdown_source").load().where("status = 'shipped'").show()

# COMMAND ----------

# MAGIC %md
# MAGIC The custom Python data source below omits `pushFilters`, so it runs on serverless. It exposes the same in-memory CH Enterprise orders, and Spark applies the `status = 'shipped'` filter after the scan.

# COMMAND ----------

from pyspark.sql.datasource import DataSource, DataSourceReader


class ChOrdersDataSource(DataSource):
    @classmethod
    def name(cls):
        return "ch_orders_source"

    def schema(self):
        return "order_id int, customer_id int, product string, quantity int, amount double, status string"

    def reader(self, schema):
        return ChOrdersReader()


class ChOrdersReader(DataSourceReader):
    def __init__(self):
        self._rows = [
            (1001, 1, "Pallet Wrap", 120, 899.50, "shipped"),
            (1002, 2, "Steel Coils", 5, 15200.00, "processing"),
            (1003, 3, "Retail Shelving", 40, 3120.75, "shipped"),
            (1004, 4, "Packaging Tape", 300, 410.00, "cancelled"),
            (1005, 1, "Pallet Wrap", 60, 449.75, "processing"),
        ]

    def read(self, partition):
        for row in self._rows:
            yield row


spark.dataSource.register(ChOrdersDataSource)
custom_source_df = spark.read.format("ch_orders_source").load()
custom_source_df.where("status = 'shipped'").show()

# COMMAND ----------

# MAGIC %md
# MAGIC ## 6. Spark Connect and Spark ML GA
# MAGIC
# MAGIC Spark ML on Spark Connect is now GA for Python clients, so the same `pyspark.ml` estimators work whether the notebook is attached to classic compute or connected remotely (Databricks Connect, serverless notebooks). A new model size estimation mechanism keeps small models cached in driver memory and spills larger ones to disk automatically, with no code change required. Spark 4.1 also improves Spark Connect's scalability and stability: compressed (zstd) protobuf logical plans, chunked Arrow result streaming over gRPC, and removal of the 2 GB limit on local relations. These are internal engine improvements, not something you configure from a notebook cell.
# MAGIC
# MAGIC The demo below trains a small ALS recommender on the CH Enterprise product ratings table; the estimator code is identical whether it runs on classic compute or over Spark Connect.
# MAGIC
# MAGIC **Serverless note:** `fit` and `transform` run on serverless. `ALSModel.recommendForAllUsers` relies on Spark higher-order functions, which are not supported on Unity Catalog serverless compute (`UC_COMMAND_NOT_SUPPORTED`), so it is shown as reference below; run it on a classic cluster to generate top-N recommendations. Here we score the training pairs with `transform` instead.

# COMMAND ----------

from pyspark.ml.recommendation import ALS

ratings_df = spark.table(f"{catalog}.{schema}.ch_product_ratings")

als = ALS(
    userCol="user_id",
    itemCol="item_id",
    ratingCol="rating",
    rank=10,
    maxIter=5,
    regParam=0.1,
    coldStartStrategy="drop",
)
als_model = als.fit(ratings_df)
als_model.transform(ratings_df).orderBy("user_id", "item_id").show(truncate=False)

# Illustrative only - recommendForAllUsers uses higher-order functions, unsupported on
# Unity Catalog serverless compute; run this on a classic Databricks Runtime 18.0 cluster:
# als_model.recommendForAllUsers(2).show(truncate=False)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 7. SQL Scripting Is GA
# MAGIC
# MAGIC SQL Scripting (`BEGIN ... END` compound statements with `DECLARE`, `SET`, loops, and conditionals) is generally available and enabled by default in Spark 4.1. New in this release: `CONTINUE HANDLER` for structured error recovery, and multi-variable `DECLARE` statements. The script below declares two variables in one statement, loops with `WHILE`, deliberately triggers a divide-by-zero, and recovers from it with a `CONTINUE HANDLER` instead of failing the whole script.
# MAGIC
# MAGIC **Version note:** run this as its own `%sql` cell; SQL Scripting compound statements are not valid inside a single `SELECT`.

# COMMAND ----------

# MAGIC %sql
# MAGIC BEGIN
# MAGIC   DECLARE i, total_orders INT DEFAULT 0;
# MAGIC   DECLARE fallback_ratio DOUBLE DEFAULT 0.0;
# MAGIC   DECLARE CONTINUE HANDLER FOR DIVIDE_BY_ZERO
# MAGIC   BEGIN
# MAGIC     SET fallback_ratio = -1.0;
# MAGIC   END;
# MAGIC
# MAGIC   WHILE i < 5 DO
# MAGIC     SET i = i + 1;
# MAGIC     SET total_orders = total_orders + i;
# MAGIC   END WHILE;
# MAGIC
# MAGIC   SET fallback_ratio = total_orders / 0;
# MAGIC
# MAGIC   SELECT total_orders AS running_total, fallback_ratio AS fallback_ratio;
# MAGIC END;

# COMMAND ----------

# MAGIC %md
# MAGIC ## 8. VARIANT Data Type Is GA
# MAGIC
# MAGIC The `VARIANT` type is now GA: a standardized way to store and query semi-structured JSON without a rigid schema, using `PARSE_JSON` to ingest and `:` path syntax to query. Spark can also shred a `VARIANT` column, pulling frequently accessed fields out into typed Parquet columns behind the scenes.
# MAGIC
# MAGIC **Performance note:** per the article, shredded VARIANT reads can be up to 8x faster than non-shredded VARIANT and up to 30x faster than parsing raw JSON strings, at the cost of roughly 20 to 50 percent slower writes. Shredding is automatic and does not change the query syntax below.

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE testing.default.ch_events (
# MAGIC   event_id INT,
# MAGIC   attributes VARIANT
# MAGIC ) USING DELTA;
# MAGIC
# MAGIC INSERT INTO testing.default.ch_events VALUES
# MAGIC   (1, PARSE_JSON('{"type": "view", "meta": {"page": "home"}}')),
# MAGIC   (2, PARSE_JSON('{"type": "click", "meta": {"page": "pricing"}}')),
# MAGIC   (3, PARSE_JSON('{"type": "view", "meta": {"page": "pricing"}}'));

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT event_id, attributes:meta.page AS page
# MAGIC FROM testing.default.ch_events
# MAGIC WHERE attributes:type::string = 'view';

# COMMAND ----------

# MAGIC %md
# MAGIC ## 9. Recursive Common Table Expressions
# MAGIC
# MAGIC Recursive CTEs (`WITH RECURSIVE`) are now supported for hierarchical queries, such as walking a manager chain, without resorting to a UDF or an external orchestration step. The example below builds the CH Enterprise org chart from `ch_employees`.
# MAGIC
# MAGIC **Version note:** recursive CTEs require Databricks Runtime 18.0 / Spark 4.1; on earlier runtimes this statement raises a syntax error.

# COMMAND ----------

# MAGIC %sql
# MAGIC WITH RECURSIVE org_tree AS (
# MAGIC   SELECT employee_id, name, manager_id, department, 0 AS depth
# MAGIC   FROM testing.default.ch_employees
# MAGIC   WHERE manager_id IS NULL
# MAGIC
# MAGIC   UNION ALL
# MAGIC
# MAGIC   SELECT e.employee_id, e.name, e.manager_id, e.department, o.depth + 1
# MAGIC   FROM testing.default.ch_employees e
# MAGIC   JOIN org_tree o ON e.manager_id = o.employee_id
# MAGIC )
# MAGIC SELECT * FROM org_tree ORDER BY depth, employee_id;

# COMMAND ----------

# MAGIC %md
# MAGIC ## 10. New Approximate Data Sketches
# MAGIC
# MAGIC Spark 4.1 adds built-in support for KLL sketches (approximate quantiles) and Theta sketches (approximate set operations such as union, intersect, and distinct counts) so large-scale approximate analytics no longer need a full sort or an exact distinct count.
# MAGIC
# MAGIC **Version note:** the exact new sketch function names are new in Spark 4.1 / Databricks Runtime 18.0 Beta and may still change between point releases; run `SHOW FUNCTIONS LIKE '*sketch*'` in your own workspace before relying on them in production. The cell below uses the stable, already-GA equivalents (`approx_percentile`, `approx_count_distinct`) so it always runs, with the new sketch-based syntax shown as a comment for reference.

# COMMAND ----------

orders_df = spark.table(f"{catalog}.{schema}.ch_orders")
orders_df.select(
    F.expr("approx_percentile(amount, 0.5)").alias("median_amount_approx"),
    F.approx_count_distinct("customer_id").alias("distinct_customers_approx"),
).show()

# Illustrative only - new KLL sketch syntax, verify the exact function names in your workspace:
# SELECT kll_sketch_estimate_quantile(kll_sketch_agg(amount), 0.5) AS median_amount
# FROM testing.default.ch_orders;

# COMMAND ----------

# MAGIC %md
# MAGIC ## Cleanup
# MAGIC
# MAGIC Drops the CH Enterprise sample tables created in Setup.

# COMMAND ----------

# MAGIC %sql
# MAGIC DROP TABLE IF EXISTS testing.default.ch_customers;
# MAGIC DROP TABLE IF EXISTS testing.default.ch_orders;
# MAGIC DROP TABLE IF EXISTS testing.default.ch_employees;
# MAGIC DROP TABLE IF EXISTS testing.default.ch_product_ratings;
# MAGIC DROP TABLE IF EXISTS testing.default.ch_events;

# COMMAND ----------

# MAGIC %md
# MAGIC **Notes:**
# MAGIC - This notebook targets Databricks Runtime 18.0 (Beta at time of writing), the first runtime to ship Apache Spark 4.1. It was validated on serverless compute; a few cells behave differently there and are called out below.
# MAGIC - Reference-only cells (shown as commented code rather than executed): the Real-Time Mode enabling flag and Kafka streaming query (needs a live Kafka broker), the Spark Declarative Pipelines decorators (run only inside a Declarative Pipeline job), the Arrow UDTF example (invocation pattern still settling between point releases), the Python worker-logging demo, the filter-pushdown data source reader, and `ALSModel.recommendForAllUsers`.
# MAGIC - Serverless limitations encountered: the configs `spark.databricks.streaming.realTimeMode.enabled` and `spark.sql.pyspark.worker.logging.enabled` cannot be set on serverless (`CONFIG_NOT_AVAILABLE`), the `spark.tvf.python_worker_logs()` TVF is not exposed, filter pushdown for Python data sources (`spark.sql.pysparkDataSourceFilterPushdown.enabled`) cannot be enabled, and `ALSModel.recommendForAllUsers` uses higher-order functions that are unsupported on Unity Catalog serverless compute. Each of these runs on a classic Databricks Runtime 18.0 cluster.
# MAGIC - The approximate data sketches section runs the stable, already-GA equivalents (`approx_percentile`, `approx_count_distinct`) and shows the new KLL sketch syntax as a comment, since the exact new function names may still change between Spark 4.1 point releases.
# MAGIC - Sample data uses the fictional company CH Enterprise, stored in `testing.default`.
