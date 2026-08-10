# Databricks notebook source
# MAGIC %md
# MAGIC # Delta Lake 4.2: VARIANT GA, SQL Schema Evolution, and Atomic RTAS for Catalog-Managed Tables
# MAGIC
# MAGIC **Article:** [Delta Lake 4.2: VARIANT GA, SQL Schema Evolution, and Atomic RTAS for Catalog-Managed Tables](https://medium.com/@cralle/what-developers-need-to-know-about-delta-lake-4-2-1c2b73dd2747?sk=a4d071df6d4b39083e2a28ebb447940e)
# MAGIC
# MAGIC **Author:** Christian Hansen ([https://medium.com/@cralle](https://medium.com/@cralle))
# MAGIC
# MAGIC **Published:** May 4, 2026
# MAGIC
# MAGIC Walks through the headline changes in Delta Lake 4.2, schema evolution that now works from pure SQL, VARIANT going GA with variant shredding, atomic RTAS and synchronous UniForm for catalog-managed tables, Delta Spark V2 streaming options, CDF quality-of-life fixes, and collations, using one continuous clickstream pipeline for a fictional company, CH Enterprise, in SQL and PySpark.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Setup
# MAGIC
# MAGIC This section creates the sample data for a CH Enterprise clickstream pipeline in `testing.default`:
# MAGIC
# MAGIC - `clickstream_raw` : a bronze landing table that already has a `device_type` column and a `raw_properties` JSON string column, standing in for whatever a real ingestion job would already be capturing.
# MAGIC - `clickstream` : a silver table that starts out **without** `device_type` or `properties`, so the schema-evolution and VARIANT sections below have something real to evolve. Change Data Feed is enabled on it from the start, so the CDF section later has a change feed to read.
# MAGIC - `clickstream_curated` : the gold table a streaming job writes to later in the notebook.
# MAGIC - a managed volume to hold the streaming checkpoint.
# MAGIC
# MAGIC Everything below assumes a Unity Catalog workspace on a Databricks Runtime that ships Delta Lake 4.2 (Delta Kernel side of things aside, the SQL/PySpark surface used here needs Databricks Runtime 17.0+ or the matching serverless compute version). Creating an ordinary managed table in Unity Catalog on such a runtime is enough to get a catalog-managed table; no extra opt-in property is needed on most workspaces at this point.

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE SCHEMA IF NOT EXISTS testing.default;

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE IF NOT EXISTS testing.default.clickstream_raw (
# MAGIC   event_date DATE,
# MAGIC   event_type STRING,
# MAGIC   user_id STRING,
# MAGIC   device_type STRING,
# MAGIC   raw_properties STRING
# MAGIC )
# MAGIC USING DELTA;
# MAGIC
# MAGIC INSERT INTO testing.default.clickstream_raw VALUES
# MAGIC   (DATE'2026-04-22', 'page_view',   'u1001', 'desktop', '{"utm_source": "direct",    "cart_value": 0.0}'),
# MAGIC   (DATE'2026-04-23', 'add_to_cart', 'u1002', 'mobile',  '{"utm_source": "email",     "cart_value": 25.0}'),
# MAGIC   (DATE'2026-04-23', 'page_view',   'u1003', 'desktop', '{"utm_source": "search",    "cart_value": 0.0}'),
# MAGIC   (DATE'2026-04-24', 'checkout',    'u1004', 'mobile',  '{"utm_source": "newsletter","cart_value": 89.99}'),
# MAGIC   (DATE'2026-04-24', 'page_view',   'u1005', 'tablet',  '{"utm_source": "direct",    "cart_value": 0.0}'),
# MAGIC   (DATE'2026-04-25', 'add_to_cart', 'u1006', 'desktop', '{"utm_source": "newsletter","cart_value": 42.50}'),
# MAGIC   (DATE'2026-04-25', 'checkout',    'u1007', 'mobile',  '{"utm_source": "social",    "cart_value": 130.0}');

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE IF NOT EXISTS testing.default.clickstream (
# MAGIC   event_date DATE,
# MAGIC   event_type STRING,
# MAGIC   user_id STRING
# MAGIC )
# MAGIC USING DELTA
# MAGIC PARTITIONED BY (event_date)
# MAGIC TBLPROPERTIES ('delta.enableChangeDataFeed' = 'true');
# MAGIC
# MAGIC -- Pre-existing rows, loaded before any of the schema evolution below happened.
# MAGIC INSERT INTO testing.default.clickstream VALUES
# MAGIC   (DATE'2026-04-22', 'page_view', 'u1001'),
# MAGIC   (DATE'2026-04-22', 'checkout',  'u1000');

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE VOLUME IF NOT EXISTS testing.default.checkpoints;

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Schema evolution, finally usable from pure SQL
# MAGIC
# MAGIC Before 4.2, automatic schema evolution worked for `INSERT INTO ... SELECT` when `delta.schemaAutoMerge.enabled` (or the equivalent session setting) was on, but `INSERT ... BY NAME` did not get the same treatment. `BY NAME` is exactly the syntax used when source and target columns do not line up positionally, which is the common case, so that gap mattered. In 4.2, `INSERT ... BY NAME` participates fully in auto-merge.
# MAGIC
# MAGIC `clickstream` currently has no `device_type` column. `clickstream_raw` does. The write below adds it in the same atomic commit as the data.
# MAGIC
# MAGIC In pure SQL the operation is a single `INSERT ... BY NAME` with auto-merge turned on for the session:
# MAGIC
# MAGIC ```sql
# MAGIC SET spark.databricks.delta.schema.autoMerge.enabled = true;
# MAGIC
# MAGIC INSERT INTO testing.default.clickstream BY NAME
# MAGIC SELECT event_date, event_type, user_id, device_type
# MAGIC FROM testing.default.clickstream_raw
# MAGIC WHERE event_date = '2026-04-23';
# MAGIC ```
# MAGIC
# MAGIC Serverless compute does not allow setting the `spark.databricks.delta.schema.autoMerge.enabled` session config, so this notebook performs the identical evolve-and-append through the DataFrame API with `.option("mergeSchema", "true")`, which is also how most ingestion jobs run it:

# COMMAND ----------

import pyspark.sql.functions as F

(spark.table("testing.default.clickstream_raw")
      .filter(F.col("event_date") == "2026-04-23")
      .select("event_date", "event_type", "user_id", "device_type")
      .write
      .format("delta")
      .mode("append")
      .option("mergeSchema", "true")
      .saveAsTable("testing.default.clickstream"))

# COMMAND ----------

# MAGIC %sql
# MAGIC -- device_type is NULL for the 2026-04-22 rows that predate the evolved schema,
# MAGIC -- and populated for the 2026-04-23 rows that just landed.
# MAGIC SELECT * FROM testing.default.clickstream ORDER BY event_date, user_id;

# COMMAND ----------

# MAGIC %md
# MAGIC The same evolve-and-append for a different partition, `2026-04-24`:

# COMMAND ----------

import pyspark.sql.functions as F

(spark.table("testing.default.clickstream_raw")
      .filter(F.col("event_date") == "2026-04-24")
      .select("event_date", "event_type", "user_id", "device_type")
      .write
      .format("delta")
      .mode("append")
      .option("mergeSchema", "true")
      .saveAsTable("testing.default.clickstream"))

display(spark.table("testing.default.clickstream").orderBy("event_date", "user_id"))

# COMMAND ----------

# MAGIC %md
# MAGIC A companion idea worth knowing: after a column is evolved into a table, it has no per-file statistics until the next `OPTIMIZE`, which silently degrades data skipping for any predicate touching that column. Some Delta builds expose a table property to force statistics collection during query optimization rather than only at write time. The exact property name is runtime/version dependent and is not recognized on every runtime (setting an unknown `delta.*` property errors unless `spark.databricks.delta.allowArbitraryProperties.enabled` is on), so it is shown here for reference rather than executed. Check the Delta documentation for the property that matches your Databricks Runtime before relying on it:
# MAGIC
# MAGIC ```sql
# MAGIC -- Shown for reference only; the exact property name is runtime/version dependent.
# MAGIC ALTER TABLE testing.default.clickstream
# MAGIC SET TBLPROPERTIES ('delta.stats.skipping.forceOptimizeStatsCollection' = 'true');
# MAGIC ```
# MAGIC
# MAGIC A simpler, always-available alternative is to run `OPTIMIZE` on the table after evolving a column so statistics are collected for the new column.

# COMMAND ----------

# MAGIC %md
# MAGIC **Trade-offs to think about.** Auto-merge is a permission, not a policy. If your governance model requires explicit schema review before columns land in a table, leave it off and use an explicit `ALTER TABLE ... ADD COLUMNS` instead. Forcing statistics collection is cheap on narrow tables but adds planning or `OPTIMIZE` overhead on tables with hundreds of columns and millions of files, so apply it per table rather than globally.

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. VARIANT goes GA: storage model, not just data type
# MAGIC
# MAGIC VARIANT stores semi-structured data, typically a JSON event payload, as a single column and lets you project into individual fields at read time. In 4.2 it is generally available in Delta Kernel, and **variant shredding** graduates from preview at the same time.
# MAGIC
# MAGIC VARIANT on its own is a self-describing binary representation of JSON, faster to traverse than re-parsing a string, but reading a hot field still walks the payload. Shredding closes that gap: once the table feature is enabled and specific paths are declared, the engine physically materializes those paths as separate Parquet columns under the hood, while the full unshredded payload stays available for everything else. Two things worth knowing that do not come up in the release notes:
# MAGIC
# MAGIC 1. **Shredding is not auto-detected.** Paths are declared through table feature configuration; the engine does not learn from query patterns.
# MAGIC 2. **Adding a shredded path does not rewrite history.** Old files still hold the path inside the variant payload. Run `OPTIMIZE` if consistent physical layout across the table matters.
# MAGIC
# MAGIC The exact table property used to declare shredded paths differs between Delta Kernel/runtime versions, so it is left out here rather than guessed. Check the Delta documentation for the syntax that matches your Databricks Runtime version before enabling it. The read-path API below, the `:` path syntax, works the same whether or not shredding is enabled underneath; the engine simply routes it through the shredded columns when they exist.
# MAGIC
# MAGIC `clickstream` still has no `properties` column. This write adds it the same way `device_type` was added above, via `mergeSchema`, while parsing the raw JSON into VARIANT on the way in.

# COMMAND ----------

(spark.table("testing.default.clickstream_raw")
      .filter(F.col("event_date") == "2026-04-25")
      .select(
          "event_date",
          "event_type",
          "user_id",
          "device_type",
          F.expr("parse_json(raw_properties)").alias("properties"),
      )
      .write
      .format("delta")
      .mode("append")
      .option("mergeSchema", "true")
      .saveAsTable("testing.default.clickstream"))

# COMMAND ----------

# MAGIC %md
# MAGIC Reading individual fields out of the VARIANT column with the `:` path syntax, cast to a concrete type with `::`:

# COMMAND ----------

shredded = (spark.table("testing.default.clickstream")
                 .filter("event_date = '2026-04-25'")
                 .selectExpr(
                     "event_date",
                     "user_id",
                     "properties:utm_source::string AS utm_source",
                     "properties:cart_value::double AS cart_value",
                 ))

display(shredded)
shredded.explain("formatted")

# COMMAND ----------

# MAGIC %md
# MAGIC Run that `explain` in your own environment once shredding is actually enabled and check the `ReadSchema` line: if the shredded columns are being projected directly, the optimization fired. That single check is the fastest way to answer "is shredding actually doing anything for us."
# MAGIC
# MAGIC **When VARIANT is the wrong tool.**
# MAGIC
# MAGIC - **Stable, narrow schemas.** If an event has a dozen fields and the contract has not changed in two years, a `STRUCT` or flat columns outperform VARIANT and give write-time type enforcement. VARIANT trades schema strictness at write for flexibility at read.
# MAGIC - **Consumers that do not speak VARIANT yet.** Delta Sharing added variantShredding support in 4.2, but downstream tools further out in the stack, BI semantic layers, third-party ETL, may still treat VARIANT as opaque. Verify before committing to it.
# MAGIC - **Spark 4.0 writers.** 4.2 explicitly blocks Spark 4.0 from writing to VARIANT tables to prevent correctness issues. Mixed-version write paths need to be resolved first.
# MAGIC - **Anywhere a `MERGE` constraint matters.** VARIANT cannot enforce that `properties:cart_value` is always a positive double. If that invariant matters, surface the field as a typed column, optionally in addition to keeping the full payload as VARIANT.
# MAGIC
# MAGIC Other VARIANT-adjacent fixes in 4.2 worth flagging: stats preservation during DML with deletion vectors (stats used to be silently dropped on `UPDATE`/`MERGE` for VARIANT columns), correct `VariantType` handling in schema conversion, and variantShredding read support in the Sharing client.

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Catalog-managed reliability: where the thesis lands hardest
# MAGIC
# MAGIC Two changes here, and both are the reason catalog-managed Delta tables stop being a roadmap promise.
# MAGIC
# MAGIC **Atomic RTAS and dynamic partition overwrite.** `REPLACE TABLE AS SELECT` and dynamic partition overwrite were not strictly atomic on catalog-managed tables in some environments before 4.2. The failure mode: an RTAS crashes mid-write, the catalog has already acknowledged the swap, the new files are partial, and readers see an empty or half-populated table while the job retries. In 4.2 both run as a single atomic commit through the catalog, so a crash mid-write leaves table state unchanged and readers never see the in-between view. This is the foundation a daily full-table refresh actually needs.
# MAGIC
# MAGIC A small daily summary table demonstrates the pattern. The first run is a plain RTAS:

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE testing.default.clickstream_daily_summary
# MAGIC USING DELTA
# MAGIC PARTITIONED BY (event_date)
# MAGIC AS
# MAGIC SELECT event_date, event_type, COUNT(*) AS event_count
# MAGIC FROM testing.default.clickstream
# MAGIC GROUP BY event_date, event_type;
# MAGIC
# MAGIC SELECT * FROM testing.default.clickstream_daily_summary ORDER BY event_date, event_type;

# COMMAND ----------

# MAGIC %md
# MAGIC Re-running the same `CREATE OR REPLACE TABLE AS SELECT` is exactly the "daily full-table refresh" pattern the article calls out: in 4.2 it commits atomically through the catalog rather than through separate metadata and data-file operations, so there is no window where readers can see a half-swapped table.
# MAGIC
# MAGIC Dynamic partition overwrite refreshes a single partition without touching the others. In SQL this is driven by the `spark.sql.sources.partitionOverwriteMode = dynamic` session config plus `INSERT OVERWRITE`; that session config is not settable on serverless compute, so the equivalent below sets `partitionOverwriteMode` as a per-write DataFrame option instead. Only the `2026-04-23` partition is rewritten:

# COMMAND ----------

(spark.sql("""
    SELECT event_date, event_type, COUNT(*) AS event_count
    FROM testing.default.clickstream
    WHERE event_date = '2026-04-23'
    GROUP BY event_date, event_type
""")
 .write
 .format("delta")
 .mode("overwrite")
 .option("partitionOverwriteMode", "dynamic")
 .saveAsTable("testing.default.clickstream_daily_summary"))

# All event_date partitions are still present; only 2026-04-23 was rewritten.
display(spark.sql("""
    SELECT event_date, COUNT(*) AS rows_for_date
    FROM testing.default.clickstream_daily_summary
    GROUP BY event_date
    ORDER BY event_date
"""))

# COMMAND ----------

# MAGIC %md
# MAGIC **Synchronous UniForm.** UniForm generates Iceberg metadata so non-Delta readers can query a Delta table without copying data. Before 4.2 that metadata generation ran in an asynchronous post-commit hook, which meant a real window between "Delta commit landed" and "Iceberg view caught up," and the source of every "why is this external engine reading stale data" investigation. In 4.2, generation moves into the commit transaction itself, so an Iceberg reader sees the commit immediately.
# MAGIC
# MAGIC The trade-off: **HMS support in UniForm is deprecated**, because the Hive Metastore has no concept of catalog-managed commits, so synchronous metadata generation cannot work there. If UniForm is still running on HMS anywhere, treat the deprecation note as a signal to migrate to a catalog that brokers commits.
# MAGIC
# MAGIC Enabling UniForm's Iceberg output is a table property change, but IcebergCompatV2 has two prerequisites that trip people up. It requires column mapping in `name` mode, and it is incompatible with deletion vectors, so any deletion vectors already on the table have to be disabled and physically purged first. On a table that was just created and refreshed like this one there are no deletion vectors to remove, but the `REORG ... APPLY (PURGE)` step is what makes the enablement safe on a table that has had `MERGE`/`UPDATE`/`DELETE` run against it:

# COMMAND ----------

# MAGIC %sql
# MAGIC -- IcebergCompatV2 is incompatible with deletion vectors: disable them and purge any that exist.
# MAGIC ALTER TABLE testing.default.clickstream_daily_summary
# MAGIC SET TBLPROPERTIES ('delta.enableDeletionVectors' = 'false');
# MAGIC
# MAGIC REORG TABLE testing.default.clickstream_daily_summary APPLY (PURGE);
# MAGIC
# MAGIC -- IcebergCompatV2 also requires column mapping in name mode.
# MAGIC ALTER TABLE testing.default.clickstream_daily_summary
# MAGIC SET TBLPROPERTIES (
# MAGIC   'delta.columnMapping.mode' = 'name',
# MAGIC   'delta.universalFormat.enabledFormats' = 'iceberg',
# MAGIC   'delta.enableIcebergCompatV2' = 'true'
# MAGIC );

# COMMAND ----------

# MAGIC %md
# MAGIC Confirming that an external Iceberg reader (Trino, for example) actually sees the metadata synchronously requires querying the table from outside Databricks, which is outside what this notebook can demonstrate; the property change above is the part that is runnable here.

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Streaming, V2, and the curated layer
# MAGIC
# MAGIC Delta Spark V2 streaming reads, still experimental in 4.2, now support every read option used in production: `startingTimestamp`, `startingVersion`, `maxBytesPerTrigger`, `maxFilesPerTrigger`, `excludeRegex`, `skipChangeCommits`, `ignoreDeletes`, `ignoreChanges`, `ignoreFileDeletion`. That removes the last reason most teams could not put a streaming source on a catalog-managed table.
# MAGIC
# MAGIC `clickstream` already has Change Data Feed enabled (it was set on the table in Setup), which the CDF quality-of-life fix below relies on.

# COMMAND ----------

# MAGIC %md
# MAGIC Silver to gold, reading from the catalog-managed silver table and writing to a curated table. `trigger(availableNow=True)` is used here so the stream processes what is currently available and stops, which is what makes this runnable inside a notebook instead of a long-running job:

# COMMAND ----------

checkpoint_path = "/Volumes/testing/default/checkpoints/clickstream_curated"

stream = (spark.readStream
               .format("delta")
               .option("startingTimestamp", "2026-01-01T00:00:00")
               .option("maxFilesPerTrigger", 200)
               .option("skipChangeCommits", "true")
               .table("testing.default.clickstream"))

query = (stream.selectExpr(
                    "event_date",
                    "event_type",
                    "device_type",
                    "properties:utm_source::string AS utm_source",
                )
                .writeStream
                .format("delta")
                .option("checkpointLocation", checkpoint_path)
                .trigger(availableNow=True)
                .toTable("testing.default.clickstream_curated"))

query.awaitTermination()

display(spark.table("testing.default.clickstream_curated").orderBy("event_date"))

# COMMAND ----------

# MAGIC %md
# MAGIC CDF also gets a related quality-of-life win in 4.2: **CDF writes for non-data-changing operations** (compactions, metadata-only commits) are now permitted, removing a whole class of phantom failures in CDC pipelines. `OPTIMIZE` on a CDF-enabled table is exactly that kind of operation:

# COMMAND ----------

# MAGIC %sql
# MAGIC OPTIMIZE testing.default.clickstream;
# MAGIC
# MAGIC -- The change feed is still consistent after a non-data-changing commit like OPTIMIZE.
# MAGIC SELECT * FROM table_changes('testing.default.clickstream', 1) LIMIT 10;

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5. Geospatial, collations, and the new Flink connector, in 60 seconds
# MAGIC
# MAGIC Three Kernel-level additions that matter mostly because they bring the rest of the ecosystem onto the same protocol.
# MAGIC
# MAGIC **Collations** add locale-aware, case-insensitive string comparison in Delta Kernel, matching what Spark already had. This one is directly runnable today:

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE testing.default.clickstream_events_ci (
# MAGIC   event_type STRING COLLATE UTF8_LCASE,
# MAGIC   user_id STRING
# MAGIC )
# MAGIC USING DELTA;
# MAGIC
# MAGIC INSERT INTO testing.default.clickstream_events_ci VALUES
# MAGIC   ('PageView', 'u2001'),
# MAGIC   ('pageview', 'u2002'),
# MAGIC   ('CHECKOUT', 'u2003');
# MAGIC
# MAGIC -- The UTF8_LCASE collation makes this comparison case-insensitive,
# MAGIC -- so both 'PageView' and 'pageview' rows match.
# MAGIC SELECT COUNT(*) AS pageview_matches
# MAGIC FROM testing.default.clickstream_events_ci
# MAGIC WHERE event_type = 'PAGEVIEW';

# COMMAND ----------

# MAGIC %md
# MAGIC **Geospatial** support lands at the protocol level with `geometry` and `geography` column types and bounding-box data skipping via `StGeometryBoxesIntersect`. This is new at the protocol level in 4.2 and not universally available yet, so it is shown here rather than run, per the note above on Preview-flag caveats:
# MAGIC
# MAGIC ```sql
# MAGIC -- Shown for reference only. Requires a Databricks Runtime / Delta protocol
# MAGIC -- combination with geospatial column support enabled; not executed in this notebook.
# MAGIC CREATE TABLE testing.default.clickstream_locations (
# MAGIC   event_id STRING,
# MAGIC   location GEOGRAPHY
# MAGIC )
# MAGIC USING DELTA;
# MAGIC ```
# MAGIC
# MAGIC The **new Apache Flink connector** is Kernel-based, supports catalog-managed tables, exactly-once writes, and the Flink Table API. It is experimental, but it replaces the connector deprecated in 4.0 and is the path forward. It is not something a Databricks SQL/PySpark notebook runs, it is configured on the Flink job itself, so it is shown for reference only:
# MAGIC
# MAGIC ```java
# MAGIC // Shown for reference only. This runs inside a Flink job, not a Databricks notebook.
# MAGIC TableEnvironment tableEnv = TableEnvironment.create(EnvironmentSettings.newInstance().build());
# MAGIC tableEnv.executeSql(
# MAGIC     "CREATE TABLE clickstream_curated " +
# MAGIC     "WITH ('connector' = 'delta', 'table-path' = 'testing.default.clickstream_curated')"
# MAGIC );
# MAGIC ```

# COMMAND ----------

# MAGIC %md
# MAGIC ## Cleanup
# MAGIC
# MAGIC Drops every table and volume created by this notebook.

# COMMAND ----------

# MAGIC %sql
# MAGIC DROP TABLE IF EXISTS testing.default.clickstream_events_ci;
# MAGIC DROP TABLE IF EXISTS testing.default.clickstream_curated;
# MAGIC DROP TABLE IF EXISTS testing.default.clickstream_daily_summary;
# MAGIC DROP TABLE IF EXISTS testing.default.clickstream;
# MAGIC DROP TABLE IF EXISTS testing.default.clickstream_raw;
# MAGIC DROP VOLUME IF EXISTS testing.default.checkpoints;

# COMMAND ----------

# MAGIC %md
# MAGIC **Source:** [Delta Lake 4.2.0 Released, delta.io blog](https://delta.io/blog/2026-04-17-delta-4-2-released/) and [Delta Lake 4.2.0 release notes, GitHub](https://github.com/delta-io/delta/releases/tag/v4.2.0).
# MAGIC
# MAGIC **Notes:** This notebook uses `testing.default` and a fictional company, CH Enterprise, in place of the `prod.consumer` namespace used in the article. The exact table property used to declare variant shredding paths is intentionally left out of Section 2 because it varies by Delta Kernel/runtime version; check the Delta documentation for the syntax that matches your Databricks Runtime before enabling it. Section 5's geospatial column type and Flink connector snippets are shown as reference code rather than executed, since geospatial columns are new at the protocol level and are not available on every workspace yet, and the Flink connector runs outside a Databricks notebook entirely. Verifying that UniForm's synchronous Iceberg metadata generation is actually visible to an external engine (Trino, for example) requires querying from outside Databricks and is likewise not something this notebook demonstrates directly.
