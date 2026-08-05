# Databricks notebook source
# MAGIC %md
# MAGIC # Apache Spark 4.2: Change Data Capture Moves Into the Engine
# MAGIC
# MAGIC Companion notebook for the Medium article. Run on **Databricks Runtime 19 (Beta)** or later, which ships Apache Spark 4.2.
# MAGIC
# MAGIC Catalog/schema used throughout: `cenh_testing.default`.
# MAGIC
# MAGIC Notes:
# MAGIC - The **Auto CDC** section uses Spark Declarative Pipelines (SDP) and must be run as a pipeline, not interactively. It is included here for reference and screenshots.
# MAGIC - All other sections are runnable interactively.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Setup: catalog, schema, and sample data
# MAGIC Creates the tables the interactive examples read from.

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE CATALOG IF NOT EXISTS cenh_testing;
# MAGIC CREATE SCHEMA IF NOT EXISTS cenh_testing.default;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Orders table for metric views and QUALIFY examples
# MAGIC CREATE OR REPLACE TABLE cenh_testing.default.orders (
# MAGIC   order_id     BIGINT,
# MAGIC   customer_id  BIGINT,
# MAGIC   order_date   DATE,
# MAGIC   region       STRING,
# MAGIC   amount       DOUBLE
# MAGIC );
# MAGIC
# MAGIC INSERT INTO cenh_testing.default.orders VALUES
# MAGIC   (1, 100, DATE'2024-02-11', 'EMEA',   120.00),
# MAGIC   (2, 100, DATE'2024-03-04', 'EMEA',    80.00),
# MAGIC   (3, 101, DATE'2024-02-19', 'AMER',   250.00),
# MAGIC   (4, 102, DATE'2024-04-01', 'APAC',    60.00),
# MAGIC   (5, 101, DATE'2024-05-22', 'AMER',   310.00),
# MAGIC   (6, 103, DATE'2024-05-30', 'EMEA',   145.00);

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Customers dimension (target of the CDC examples) with change data feed enabled
# MAGIC CREATE OR REPLACE TABLE cenh_testing.default.customers (
# MAGIC   customer_id  BIGINT,
# MAGIC   name         STRING,
# MAGIC   city         STRING
# MAGIC ) TBLPROPERTIES (delta.enableChangeDataFeed = true);
# MAGIC
# MAGIC INSERT INTO cenh_testing.default.customers VALUES
# MAGIC   (100, 'Ada Lovelace',   'London'),
# MAGIC   (101, 'Alan Turing',    'Manchester'),
# MAGIC   (102, 'Grace Hopper',   'New York');
# MAGIC
# MAGIC -- A second version so CHANGES FROM VERSION 0 TO VERSION 1 returns rows
# MAGIC UPDATE cenh_testing.default.customers SET city = 'Cambridge' WHERE customer_id = 101;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Staging table for the INSERT ... BY NAME schema evolution example
# MAGIC CREATE OR REPLACE TABLE cenh_testing.default.customers_staging (
# MAGIC   customer_id   BIGINT,
# MAGIC   name          STRING,
# MAGIC   city          STRING,
# MAGIC   loyalty_tier  STRING
# MAGIC );
# MAGIC
# MAGIC INSERT INTO cenh_testing.default.customers_staging VALUES
# MAGIC   (104, 'Katherine Johnson', 'Hampton', 'gold');

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Raw change feed that the Auto CDC pipeline consumes.
# MAGIC -- Run this here (interactive), NOT inside the pipeline.
# MAGIC CREATE OR REPLACE TABLE cenh_testing.default.customers_cdc_raw
# MAGIC AS SELECT * FROM (VALUES
# MAGIC   (100, 'Ada Lovelace', 'London',     'INSERT', 1),
# MAGIC   (101, 'Alan Turing',  'Manchester', 'INSERT', 1),
# MAGIC   (102, 'Grace Hopper', 'New York',   'INSERT', 2),
# MAGIC   (101, 'Alan Turing',  'Cambridge',  'UPDATE', 3),
# MAGIC   (102, NULL,           NULL,         'DELETE', 4)
# MAGIC ) AS t(customer_id, name, city, operation, sequence_num);

# COMMAND ----------

# MAGIC %md
# MAGIC ## Auto CDC (Spark Declarative Pipelines)
# MAGIC
# MAGIC **This does not run in an interactive notebook.** The `pyspark.pipelines` API only executes
# MAGIC inside a pipeline run. Running it here raises `PIPELINES_NOT_SUPPORTED`.
# MAGIC
# MAGIC To run it: create a Lakeflow pipeline (serverless, or Pro/Advanced edition) whose source is the
# MAGIC companion file **`auto_cdc_pipeline.py`**, set its default catalog to `cenh_testing` and schema to
# MAGIC `default`, then run the pipeline. The source table `cenh_testing.default.customers_cdc_raw` is
# MAGIC created in the setup section above. The flow below produces the streaming table
# MAGIC `cenh_testing.default.customers_current` (final state: 100 London, 101 Cambridge; 102 deleted).
# MAGIC
# MAGIC The pipeline code, shown here for reference only (do not run in this notebook):
# MAGIC
# MAGIC ```python
# MAGIC from pyspark import pipelines as dp
# MAGIC import pyspark.sql.functions as F
# MAGIC
# MAGIC @dp.view
# MAGIC def customers_changes():
# MAGIC     return spark.readStream.table("cenh_testing.default.customers_cdc_raw")
# MAGIC
# MAGIC dp.create_streaming_table("customers_current")
# MAGIC
# MAGIC dp.create_auto_cdc_flow(
# MAGIC     target="customers_current",
# MAGIC     source="customers_changes",
# MAGIC     keys=["customer_id"],
# MAGIC     sequence_by=F.col("sequence_num"),
# MAGIC     apply_as_deletes=F.expr("operation = 'DELETE'"),
# MAGIC     except_column_list=["operation", "sequence_num"],
# MAGIC     stored_as_scd_type=1,
# MAGIC )
# MAGIC ```

# COMMAND ----------

# MAGIC %md
# MAGIC ## The CHANGES clause: one way to read a change feed
# MAGIC A standard clause backed by the DSv2 CDC API. It requires the catalog/connector to implement
# MAGIC that API. On Unity Catalog managed Delta in DBR 19 Beta this may raise
# MAGIC `UNSUPPORTED_FEATURE.CHANGE_DATA_CAPTURE`. Use the `table_changes()` cell below for a runnable
# MAGIC Delta demo (Change Data Feed was enabled on the table in setup).

# COMMAND ----------

# MAGIC %sql
# MAGIC -- New standard syntax. Requires a connector that exposes the DSv2 CDC API.
# MAGIC -- May raise UNSUPPORTED_FEATURE.CHANGE_DATA_CAPTURE on UC-managed Delta in DBR 19 Beta.
# MAGIC SELECT *
# MAGIC FROM cenh_testing.default.customers
# MAGIC CHANGES FROM VERSION 0 TO VERSION 2;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Runnable equivalent for Delta today (needs delta.enableChangeDataFeed = true, set in setup).
# MAGIC -- Reads row-level changes from version 1 onward: the INSERT and the UPDATE.
# MAGIC SELECT * FROM table_changes('cenh_testing.default.customers', 1);

# COMMAND ----------

# MAGIC %md
# MAGIC ## DSv2 hardening: INSERT ... BY NAME with schema evolution
# MAGIC `loyalty_tier` does not exist on the target and is added as part of the write.

# COMMAND ----------

# MAGIC %sql
# MAGIC SET spark.databricks.delta.schema.autoMerge.enabled = true;
# MAGIC
# MAGIC INSERT INTO cenh_testing.default.customers BY NAME
# MAGIC SELECT customer_id, name, city, loyalty_tier
# MAGIC FROM cenh_testing.default.customers_staging;

# COMMAND ----------

# MAGIC %md
# MAGIC ## Metric views: define the metric once
# MAGIC A native semantic layer. Measures are computed at the correct grain no matter how they are sliced.

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE VIEW cenh_testing.default.orders_metrics
# MAGIC WITH METRICS
# MAGIC LANGUAGE YAML
# MAGIC AS $$
# MAGIC version: 1.1
# MAGIC source: cenh_testing.default.orders
# MAGIC filter: order_date > '2024-01-01'
# MAGIC fields:
# MAGIC   - name: order_month
# MAGIC     expr: DATE_TRUNC('MONTH', order_date)
# MAGIC   - name: region
# MAGIC     expr: region
# MAGIC measures:
# MAGIC   - name: total_revenue
# MAGIC     expr: SUM(amount)
# MAGIC   - name: distinct_customers
# MAGIC     expr: COUNT(DISTINCT customer_id)
# MAGIC $$;

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT region, MEASURE(distinct_customers) AS customers
# MAGIC FROM cenh_testing.default.orders_metrics
# MAGIC GROUP BY region;

# COMMAND ----------

# MAGIC %md
# MAGIC ## A more Arrow-first Python path
# MAGIC Arrow-optimized Python UDFs are the default in 4.2. `useArrow=True` is explicit here for illustration.

# COMMAND ----------

import pyspark.sql.functions as F

# Arrow is the default in 4.2; this is explicit for illustration
@F.udf(returnType="int", useArrow=True)
def name_length(s):
    return len(s) if s else 0

df = spark.table("cenh_testing.default.customers")
df.select(name_length("name").alias("name_len")).show()

# COMMAND ----------

# MAGIC %md
# MAGIC ## SQL highlights: QUALIFY
# MAGIC Filter window function results without a subquery. Classic "latest row per key".

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT customer_id, order_date, amount
# MAGIC FROM cenh_testing.default.orders
# MAGIC QUALIFY ROW_NUMBER() OVER (
# MAGIC   PARTITION BY customer_id ORDER BY order_date DESC
# MAGIC ) = 1;

# COMMAND ----------

# MAGIC %md
# MAGIC ## SQL highlights: native geospatial types
# MAGIC Built-in GEOMETRY/GEOGRAPHY types with construction and SRID functions.

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Point (1, 2) in WKB, parsed as GEOGRAPHY (SRID 4326), then round-tripped to WKB
# MAGIC SELECT
# MAGIC   ST_Srid(ST_GeogFromWKB(X'0101000000000000000000F03F0000000000000040'))          AS srid,
# MAGIC   hex(ST_AsBinary(ST_GeogFromWKB(X'0101000000000000000000F03F0000000000000040'))) AS wkb_hex;

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC **Notes and sources.** Auto CDC (`create_auto_cdc_flow`) runs only inside a Spark Declarative
# MAGIC Pipeline and replaces the earlier `apply_changes()`. The `CHANGES` clause requires a connector
# MAGIC that implements the DSv2 CDC API. Most of these features ship first in Databricks Runtime 19 (Beta).
# MAGIC
# MAGIC Sources: Introducing Apache Spark 4.2 (Databricks blog), Spark 4.2.0 release notes (apache.org),
# MAGIC create_auto_cdc_flow reference and Unity Catalog metric views (Databricks docs),
# MAGIC Data Source V2 and Geospatial types (Spark 4.2 docs).
