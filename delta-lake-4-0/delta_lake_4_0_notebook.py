# Databricks notebook source
# MAGIC %md
# MAGIC # What Developers Need to Know About Delta Lake 4.0
# MAGIC
# MAGIC **Article:** [What Developers Need to Know About Delta Lake 4.0](https://medium.com/@cralle/what-developers-need-to-know-about-delta-lake-4-0-79489eb8cf9e?sk=864633b331861d0715e6abb1870e5fab)
# MAGIC
# MAGIC **Author:** Christian Hansen (https://medium.com/@cralle)
# MAGIC
# MAGIC **Published:** Sep 11, 2025
# MAGIC
# MAGIC A tour of the preview release of Delta Lake 4.0, the largest release in Delta Lake's history, built on Apache Spark 4.0: Delta Connect, Coordinated Commits, the Variant data type, Type Widening, Identity Columns and Collations (both marked Coming Soon at the time of writing), UniForm reaching general availability, Delta Kernel, Delta Rust 1.0, and a handful of smaller wins, each demonstrated against small CH Enterprise sample tables.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Setup
# MAGIC
# MAGIC Creates the `testing.default` catalog/schema (if they do not already exist) and three foundational CH Enterprise sample tables used throughout this notebook: a customers table (Type Widening demo), a raw sensor-readings table (Variant demo), and an orders table (Coordinated Commits, generated columns, liquid clustering, and CDF demos). The Identity Columns, Collations, UniForm, and Delta Rust sections create their own small demo tables further down, since each of those is self-contained.

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE CATALOG IF NOT EXISTS testing;
# MAGIC CREATE SCHEMA IF NOT EXISTS testing.default;
# MAGIC USE CATALOG testing;
# MAGIC USE SCHEMA default;

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE testing.default.ch_enterprise_customers (
# MAGIC   customer_id INT,
# MAGIC   customer_name STRING,
# MAGIC   signup_date DATE
# MAGIC )
# MAGIC TBLPROPERTIES ('delta.enableTypeWidening' = 'true');
# MAGIC
# MAGIC INSERT INTO testing.default.ch_enterprise_customers VALUES
# MAGIC   (1001, 'Nordic Retail A/S', DATE'2024-01-15'),
# MAGIC   (1002, 'Baltic Freight OY', DATE'2024-03-02'),
# MAGIC   (1003, 'Harbor Logistics Inc', DATE'2024-06-20');

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE testing.default.ch_enterprise_sensor_readings_raw (
# MAGIC   event_id BIGINT,
# MAGIC   sensor_id STRING,
# MAGIC   event_ts TIMESTAMP,
# MAGIC   raw_payload STRING
# MAGIC );
# MAGIC
# MAGIC INSERT INTO testing.default.ch_enterprise_sensor_readings_raw VALUES
# MAGIC   (1, 'SENSOR-042', TIMESTAMP'2026-08-01 09:00:00',
# MAGIC    '{"model": "TH-9000", "location": {"site": "Odense", "floor": 3}, "calibrated": true}'),
# MAGIC   (2, 'SENSOR-107', TIMESTAMP'2026-08-01 09:05:00',
# MAGIC    '{"model": "TH-9100", "location": {"site": "Aarhus", "floor": 1}, "calibrated": false}');

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE testing.default.ch_enterprise_orders (
# MAGIC   order_id BIGINT,
# MAGIC   customer_id INT,
# MAGIC   order_amount DECIMAL(10,2),
# MAGIC   created_ts TIMESTAMP,
# MAGIC   expires_ts TIMESTAMP GENERATED ALWAYS AS (timestampadd(DAY, 30, created_ts))
# MAGIC )
# MAGIC CLUSTER BY (customer_id)
# MAGIC TBLPROPERTIES ('delta.enableChangeDataFeed' = 'true');
# MAGIC
# MAGIC INSERT INTO testing.default.ch_enterprise_orders (order_id, customer_id, order_amount, created_ts) VALUES
# MAGIC   (5001, 1001, 4820.50, TIMESTAMP'2026-07-01 10:00:00'),
# MAGIC   (5002, 1002, 990.00, TIMESTAMP'2026-07-03 14:30:00'),
# MAGIC   (5003, 1001, 15200.75, TIMESTAMP'2026-07-10 08:15:00');

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Delta Connect (Spark Connect Support)
# MAGIC
# MAGIC Delta Connect adds full Spark Connect support to Delta Lake: a decoupled client-server architecture where a thin client sends unresolved logical plans to a remote Spark Connect server, which does all the actual Delta table access. Getting a Delta Connect session running means starting a separate `delta-connect-server_2.13:4.0.0` process (with `DeltaSparkSessionExtension`, `DeltaCatalog`, `DeltaRelationPlugin`, and `DeltaCommandPlugin` configured) and connecting to it from a client that has `pyspark==4.0.0` and `delta-spark==4.0.0` installed.
# MAGIC
# MAGIC **Caveat:** this is a two-process architecture (server plus client), so it cannot be exercised from inside a single Databricks notebook cell the way a table operation can. A Databricks notebook already runs against a managed Spark cluster, not against a standalone Delta Connect server. The client-side code is shown below for reference only, not executed.
# MAGIC
# MAGIC ```python
# MAGIC # Reference only - requires a separate delta-connect-server process reachable at <host:port>
# MAGIC from delta.tables import DeltaTable
# MAGIC from pyspark.sql import SparkSession
# MAGIC
# MAGIC spark = SparkSession.builder.remote("sc://<host:port>").getOrCreate()
# MAGIC delta_table = DeltaTable.forName(spark, "testing.default.ch_enterprise_orders")
# MAGIC delta_table.toDF().show()
# MAGIC history_df = delta_table.history()
# MAGIC ```

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Coordinated Commits
# MAGIC
# MAGIC Coordinated Commits introduces a single point of ownership for commits to a table: a Commit Coordinator, identified by a unique ID, that every writer must go through. This fixes three gaps in the older filesystem-based commit protocol: catalogs could not participate in commits, no ownership was tied to a table (different clusters could point at different LogStores and lose or corrupt commits), and there was no way to commit atomically across multiple tables.
# MAGIC
# MAGIC **Caveat:** on Databricks, Unity Catalog managed tables like `ch_enterprise_orders` above are already catalog-owned end to end, so there is no separate coordinator configuration to toggle by hand here. The demo below simply inspects the managed table's detail and properties to show that ownership already lives with the catalog rather than with any one cluster's LogStore configuration.

# COMMAND ----------

# MAGIC %sql
# MAGIC DESCRIBE DETAIL testing.default.ch_enterprise_orders;

# COMMAND ----------

# MAGIC %sql
# MAGIC SHOW TBLPROPERTIES testing.default.ch_enterprise_orders;

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Variant Type
# MAGIC
# MAGIC `VARIANT` is a new data type for semi-structured and hierarchical data such as JSON. It avoids the usual trade-off between a loose JSON string column and a rigid struct schema: you can ingest without committing to a schema up front while still getting much better performance than scanning raw JSON strings. It requires Reader version 3, Writer version 7, and the `variantType` reader/writer feature, which Databricks enables automatically the first time a `VARIANT` column is written.
# MAGIC
# MAGIC **Feature support matrix**
# MAGIC
# MAGIC | Feature | Support |
# MAGIC | --- | --- |
# MAGIC | Partition columns | Allowed as a regular column; not allowed as a partition key (Variant is not comparable) |
# MAGIC | Clustered tables | Allowed as a regular column; not allowed as a clustering key |
# MAGIC | Column statistics | `nullCount` collected; min/max not collected |
# MAGIC | Generated columns | Allowed as a source column; not allowed as the generated column's result type |
# MAGIC | CHECK constraints | Supported |
# MAGIC | Default values | Supported |
# MAGIC | Change Data Feed | Supported |
# MAGIC
# MAGIC The cell below builds a `VARIANT` table from the raw JSON sensor payloads created in Setup, using `PARSE_JSON`.

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE testing.default.ch_enterprise_sensor_readings_variant AS
# MAGIC SELECT
# MAGIC   event_id,
# MAGIC   sensor_id,
# MAGIC   event_ts,
# MAGIC   PARSE_JSON(raw_payload) AS sensor_master_data
# MAGIC FROM testing.default.ch_enterprise_sensor_readings_raw;
# MAGIC
# MAGIC SELECT
# MAGIC   event_id,
# MAGIC   sensor_id,
# MAGIC   sensor_master_data:model,
# MAGIC   sensor_master_data:location.site,
# MAGIC   sensor_master_data:calibrated
# MAGIC FROM testing.default.ch_enterprise_sensor_readings_variant
# MAGIC ORDER BY event_id;

# COMMAND ----------

import pyspark.sql.functions as F

variant_df = spark.table("testing.default.ch_enterprise_sensor_readings_variant")

# variant_get() pulls a typed value out of the VARIANT column by path
variant_df.select(
    "event_id",
    "sensor_id",
    F.variant_get(F.col("sensor_master_data"), "$.location.site", "string").alias("site"),
    F.variant_get(F.col("sensor_master_data"), "$.calibrated", "boolean").alias("calibrated"),
).orderBy("event_id").display()

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Type Widening
# MAGIC
# MAGIC Type Widening lets you change a column to a wider type on an existing table, in place, instead of rewriting it into a new one. Supported conversions include Byte -> Short -> Int -> Long, Float -> Double, `Decimal(p,s)` -> `Decimal(p+k1, s+k2)` where `k1 >= k2 >= 0`, and Date -> Timestamp (without time zone). It requires Reader version 3, Writer version 7, and the `typeWidening` table feature, enabled with `'delta.enableTypeWidening' = 'true'` (already set on `ch_enterprise_customers` in Setup); writers reject a widening attempt if the property is not set. Every widening is recorded in the schema under `delta.typeChanges`, together with the table version it happened at, giving a full audit trail.
# MAGIC
# MAGIC The cell below widens `customer_id` from `INT` to `BIGINT`.

# COMMAND ----------

# MAGIC %sql
# MAGIC ALTER TABLE testing.default.ch_enterprise_customers
# MAGIC ALTER COLUMN customer_id TYPE BIGINT;

# COMMAND ----------

# MAGIC %sql
# MAGIC DESCRIBE TABLE testing.default.ch_enterprise_customers;

# COMMAND ----------

from delta.tables import DeltaTable

customers_table = DeltaTable.forName(spark, "testing.default.ch_enterprise_customers")
customers_table.toDF().printSchema()

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5. Identity Columns
# MAGIC
# MAGIC Identity Columns give every row an auto-incrementing ID without any manual ID generation logic, which simplifies relational modeling and gives consistent, unique keys across writers.
# MAGIC
# MAGIC **Caveat:** at the time this article was written, Identity Columns for the open source Delta Lake table format itself were marked "Coming Soon," not yet part of the 4.0 preview. Databricks has separately supported `GENERATED ... AS IDENTITY` columns on Delta tables for a while as a platform feature, independent of the open source project's release cadence. The demo below uses that existing Databricks capability to illustrate the same idea; it is not evidence that the OSS table feature has shipped.

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE testing.default.ch_enterprise_identity_demo (
# MAGIC   ticket_id BIGINT GENERATED BY DEFAULT AS IDENTITY,
# MAGIC   subject STRING
# MAGIC );
# MAGIC
# MAGIC INSERT INTO testing.default.ch_enterprise_identity_demo (subject) VALUES
# MAGIC   ('Sensor offline at Odense site'),
# MAGIC   ('Shipment delayed: Aarhus to Rotterdam');
# MAGIC
# MAGIC SELECT * FROM testing.default.ch_enterprise_identity_demo ORDER BY ticket_id;

# COMMAND ----------

# MAGIC %md
# MAGIC ## 6. Collations
# MAGIC
# MAGIC Collations let you specify how a `STRING` column is compared and sorted: case sensitivity, accent sensitivity, and language- or region-aware ordering, kept consistent across engines.
# MAGIC
# MAGIC **Caveat:** as with Identity Columns, Collations for the open source Delta Lake table format were marked "Coming Soon" in this article, not yet part of the 4.0 preview. Databricks already supports `COLLATE` on `STRING` values as a platform feature. The comparison below uses that existing support to show the concept; it does not depend on the OSS table feature.

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC   'CH Enterprise' = 'ch enterprise' AS binary_compare_is_case_sensitive,
# MAGIC   'CH Enterprise' COLLATE UTF8_LCASE = 'ch enterprise' COLLATE UTF8_LCASE AS lcase_compare_is_case_insensitive;

# COMMAND ----------

# MAGIC %md
# MAGIC ## 7. UniForm (GA)
# MAGIC
# MAGIC UniForm, the Universal Format, reached general availability in this release. Delta, Iceberg, and Hudi all store the same Parquet data files plus their own metadata layer on top; UniForm auto-generates Iceberg- and Hudi-compatible metadata for a Delta table, asynchronously after each Delta commit, so you write once with Delta and read natively from Iceberg or Hudi clients. It is enabled with table properties: `'delta.enableIcebergCompatV2' = 'true'` plus `'delta.universalFormat.enabledFormats' = 'iceberg'` for Iceberg, `'delta.universalFormat.enabledFormats' = 'hudi'` for Hudi, or `'iceberg,hudi'` for both. `enableIcebergCompatV2` requires column mapping mode `name`.

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE testing.default.ch_enterprise_shipments (
# MAGIC   shipment_id BIGINT,
# MAGIC   origin STRING,
# MAGIC   destination STRING,
# MAGIC   shipped_at TIMESTAMP
# MAGIC )
# MAGIC TBLPROPERTIES (
# MAGIC   'delta.enableIcebergCompatV2' = 'true',
# MAGIC   'delta.columnMapping.mode' = 'name',
# MAGIC   'delta.universalFormat.enabledFormats' = 'iceberg'
# MAGIC );
# MAGIC
# MAGIC INSERT INTO testing.default.ch_enterprise_shipments VALUES
# MAGIC   (9001, 'Odense', 'Hamburg', TIMESTAMP'2026-08-01 07:30:00'),
# MAGIC   (9002, 'Aarhus', 'Rotterdam', TIMESTAMP'2026-08-01 08:15:00');

# COMMAND ----------

# MAGIC %md
# MAGIC The open source Delta Lake 4.0 project accepts `iceberg`, `hudi`, or `iceberg,hudi` as UniForm target formats. On Databricks, `delta.universalFormat.enabledFormats` currently accepts only `iceberg` (and the internal `compatibility` value), so the multi-format `ALTER` that adds Hudi is shown for reference only; Iceberg UniForm is already turned on by the `CREATE TABLE` above and confirmed by the `SHOW TBLPROPERTIES` output below.
# MAGIC
# MAGIC ```sql
# MAGIC -- Reference only - Databricks accepts iceberg (and compatibility); Hudi UniForm output is an open source Delta Lake 4.0 capability
# MAGIC ALTER TABLE testing.default.ch_enterprise_shipments SET TBLPROPERTIES (
# MAGIC   'delta.universalFormat.enabledFormats' = 'iceberg,hudi'
# MAGIC );
# MAGIC ```

# COMMAND ----------

# MAGIC %sql
# MAGIC SHOW TBLPROPERTIES testing.default.ch_enterprise_shipments;

# COMMAND ----------

# MAGIC %md
# MAGIC **Caveat:** the `CREATE TABLE` statement above is what actually turns UniForm on and is run as normal SQL. Reading the resulting table back with a real Iceberg or Hudi client is shown for reference only, since it needs that client's connector library on the cluster rather than anything Delta Lake provides:
# MAGIC
# MAGIC ```python
# MAGIC # Reference only - requires the Hudi Spark connector library on the cluster
# MAGIC hudi_df = (
# MAGIC     spark.read.format("hudi")
# MAGIC     .option("hoodie.metadata.enable", "true")
# MAGIC     .load("<storage path of ch_enterprise_shipments>")
# MAGIC )
# MAGIC ```

# COMMAND ----------

# MAGIC %md
# MAGIC ## 8. Delta Kernel
# MAGIC
# MAGIC Delta Kernel is a lightweight library for reading and writing Delta tables, with Java and Rust implementations, aimed at people building connectors rather than at Spark users. It exposes narrow Table APIs (`Table`, `Snapshot`) and pluggable Engine APIs (for example, a custom Parquet reader), so a connector author does not need to implement the full Delta protocol from scratch.
# MAGIC
# MAGIC **Caveat:** Delta Kernel is a Java/Rust library for building connectors outside of Spark; there is no PySpark or SQL entry point to call from a Databricks notebook, so there is no runnable demo for this item here. The shape of a Kernel-based scan is shown for reference only:
# MAGIC
# MAGIC ```java
# MAGIC // Reference only - Delta Kernel Java API, not callable from a Databricks notebook
# MAGIC Engine engine = DefaultEngine.create(new Configuration());
# MAGIC Table table = Table.forPath(engine, "<path to a Delta table>");
# MAGIC Snapshot snapshot = table.getLatestSnapshot(engine);
# MAGIC Scan scan = snapshot.getScanBuilder(engine).withFilter(engine, someFilterExpr).build();
# MAGIC ```

# COMMAND ----------

# MAGIC %md
# MAGIC ## 9. Delta Rust 1.0 (delta-rs)
# MAGIC
# MAGIC Delta Rust is the community-maintained Rust and Python implementation of Delta Lake, now production-ready at 1.0: Change Data Feed support, constraints, schema evolution for all Rust/Python writers, deletion vectors, a Rust-based writer engine used by default from Python, a stabilized DataFusion API, and stabilized object store configuration via environment variables.
# MAGIC
# MAGIC `delta-rs` talks to storage directly through the `deltalake` Python package; it does not go through Spark or Unity Catalog, so the demo below writes a small path-based table rather than registering a catalog table.
# MAGIC
# MAGIC **Caveat:** the path used here is ephemeral local disk on the cluster driver, chosen to keep this notebook self-contained; point `path` at a Unity Catalog volume or cloud storage location instead for anything durable or shared.

# COMMAND ----------

# MAGIC %pip install deltalake

# COMMAND ----------

dbutils.library.restartPython()

# COMMAND ----------

import pandas as pd
from deltalake import DeltaTable, write_deltalake

delta_rs_demo_path = "/tmp/ch_enterprise_delta_rs_demo"

ch_enterprise_carriers = pd.DataFrame(
    {
        "carrier_id": [1, 2],
        "carrier_name": ["Maersk", "DHL Freight"],
    }
)

write_deltalake(delta_rs_demo_path, ch_enterprise_carriers)

carriers_table = DeltaTable(delta_rs_demo_path)
print(carriers_table.to_pandas())
print("table version:", carriers_table.version())

# COMMAND ----------

# MAGIC %md
# MAGIC ## 10. Other Notable Changes
# MAGIC
# MAGIC A set of smaller wins that shipped alongside the headline features.

# COMMAND ----------

# MAGIC %md
# MAGIC ### 10.1 Liquid clustering fix: single-column fallback to Z-order
# MAGIC
# MAGIC `ch_enterprise_orders` is clustered on a single column (`customer_id`). Delta Lake 4.0 fixes the case where clustering on a single column previously behaved inconsistently, so it now falls back to Z-ordering internally instead. Running `OPTIMIZE` triggers this path; the fix itself is internal, so there is nothing extra to configure.

# COMMAND ----------

# MAGIC %sql
# MAGIC OPTIMIZE testing.default.ch_enterprise_orders;

# COMMAND ----------

# MAGIC %md
# MAGIC ### 10.2 Generated column enhancements: `timestampadd` and `timestampdiff`
# MAGIC
# MAGIC Generated columns can now use `timestampadd` and `timestampdiff` in their expressions. `ch_enterprise_orders.expires_ts` was defined in Setup as `GENERATED ALWAYS AS (timestampadd(DAY, 30, created_ts))`; the cell below confirms every row's `expires_ts` really is `created_ts` plus 30 days.

# COMMAND ----------

import pyspark.sql.functions as F

orders_df = spark.table("testing.default.ch_enterprise_orders")
orders_df.select(
    "order_id",
    "created_ts",
    "expires_ts",
    F.datediff(F.col("expires_ts"), F.col("created_ts")).alias("days_until_expiry"),
).orderBy("order_id").display()

# COMMAND ----------

# MAGIC %md
# MAGIC ### 10.3 `CREATE TABLE ... LIKE` respects user-provided table properties
# MAGIC
# MAGIC Delta Lake 4.0 fixes `CREATE TABLE ... LIKE` so that table properties you pass explicitly are respected instead of always being overridden by the source table's properties. The archive table below copies the schema of `ch_enterprise_orders` (which has Change Data Feed enabled) but explicitly disables it.

# COMMAND ----------

# MAGIC %sql
# MAGIC DROP TABLE IF EXISTS testing.default.ch_enterprise_orders_archive;
# MAGIC
# MAGIC CREATE TABLE testing.default.ch_enterprise_orders_archive
# MAGIC LIKE testing.default.ch_enterprise_orders
# MAGIC TBLPROPERTIES ('delta.enableChangeDataFeed' = 'false');
# MAGIC
# MAGIC INSERT INTO testing.default.ch_enterprise_orders_archive
# MAGIC SELECT * FROM testing.default.ch_enterprise_orders;
# MAGIC
# MAGIC SHOW TBLPROPERTIES testing.default.ch_enterprise_orders_archive;

# COMMAND ----------

# MAGIC %md
# MAGIC ### 10.4 Partition-level sorting for Z-ordering
# MAGIC
# MAGIC `OPTIMIZE ... ZORDER BY` can now sort within each partition before writing, controlled by `spark.databricks.io.skipping.mdc.sortWithinPartitions` (disabled by default). `ch_enterprise_customers` has no clustering key defined, so it is a plain candidate for Z-ordering.
# MAGIC
# MAGIC **Caveat:** that Spark configuration is not settable on serverless compute (it returns `CONFIG_NOT_AVAILABLE`), so the `SET` is shown for reference only; on classic compute you would run it before the `OPTIMIZE` below. The `OPTIMIZE ... ZORDER BY` itself runs as normal.
# MAGIC
# MAGIC ```sql
# MAGIC -- Reference only - not settable on serverless compute; run on classic compute to enable partition-level sorting
# MAGIC SET spark.databricks.io.skipping.mdc.sortWithinPartitions = true;
# MAGIC ```

# COMMAND ----------

# MAGIC %sql
# MAGIC OPTIMIZE testing.default.ch_enterprise_customers ZORDER BY (customer_id);

# COMMAND ----------

# MAGIC %md
# MAGIC ### 10.5 Protocol version downgrades preserve existing table features
# MAGIC
# MAGIC Dropping a table feature with `ALTER TABLE ... DROP FEATURE` can downgrade the table's protocol version; 4.0 fixes this path so that a downgrade no longer silently drops other, unrelated table features that are still in use. `DROP FEATURE` also requires the feature to be unused within the table's log retention window (or an explicit `TRUNCATE HISTORY`), which makes it a poor fit for a notebook meant to run cleanly end to end on a freshly created table, so it is shown for reference only rather than executed against the sample tables above:
# MAGIC
# MAGIC ```sql
# MAGIC -- Reference only - depends on log retention history, not run against the sample tables above
# MAGIC ALTER TABLE testing.default.ch_enterprise_orders_archive
# MAGIC DROP FEATURE changeDataFeed TRUNCATE HISTORY;
# MAGIC ```

# COMMAND ----------

# MAGIC %md
# MAGIC ### 10.6 CDF filter pushdown
# MAGIC
# MAGIC Reading a Change Data Feed with a filter can now push that filter down into partition pruning and Parquet row-group skipping, instead of reading every changed file and filtering afterward. The query shape below is what triggers the pushdown; the win is in how much data gets scanned, not in the result itself.

# COMMAND ----------

import pyspark.sql.functions as F

order_changes_df = (
    spark.read.format("delta")
    .option("readChangeFeed", "true")
    .option("startingVersion", 0)
    .table("testing.default.ch_enterprise_orders")
)

order_changes_df.filter(F.col("customer_id") == 1001).select(
    "order_id", "customer_id", "_change_type", "_commit_version"
).display()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Cleanup
# MAGIC
# MAGIC Drops every table created during Setup and the numbered sections above, and removes the ephemeral local path used by the Delta Rust demo.

# COMMAND ----------

# MAGIC %sql
# MAGIC DROP TABLE IF EXISTS testing.default.ch_enterprise_orders_archive;
# MAGIC DROP TABLE IF EXISTS testing.default.ch_enterprise_shipments;
# MAGIC DROP TABLE IF EXISTS testing.default.ch_enterprise_identity_demo;
# MAGIC DROP TABLE IF EXISTS testing.default.ch_enterprise_sensor_readings_variant;
# MAGIC DROP TABLE IF EXISTS testing.default.ch_enterprise_sensor_readings_raw;
# MAGIC DROP TABLE IF EXISTS testing.default.ch_enterprise_orders;
# MAGIC DROP TABLE IF EXISTS testing.default.ch_enterprise_customers;

# COMMAND ----------

dbutils.fs.rm("/tmp/ch_enterprise_delta_rs_demo", True)

# COMMAND ----------

# MAGIC %md
# MAGIC **Notes:**
# MAGIC - This article covers the preview release of Delta Lake 4.0 (the open source project), announced June 2025 and built on Apache Spark 4.0. Databricks Runtime often carries its own, separately timed implementation of a given Delta capability (Variant, Type Widening, Liquid Clustering, UniForm, and native IDENTITY/COLLATE support all predate or track ahead of the OSS release used in specific sections here); this notebook runs on Databricks Runtime and calls that out inline wherever the two timelines diverge, most notably for Identity Columns and Collations, which were marked "Coming Soon" for the OSS table format at the time of writing.
# MAGIC - Delta Connect and Delta Kernel are shown as reference code only: Delta Connect needs a separate client/server topology, and Delta Kernel is a Java/Rust library with no PySpark or SQL entry point, so neither can run as a normal notebook cell.
# MAGIC - The article includes an Oct 8, 2025 edit noting that Databricks Runtime 17.3 LTS (which carries Delta Lake/Spark 4.0-era functionality) began rolling out after this piece was first published; prefer an LTS runtime for anything you plan to keep running long term.
