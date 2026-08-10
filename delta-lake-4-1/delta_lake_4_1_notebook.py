# Databricks notebook source
# MAGIC %md
# MAGIC # What Developers Need to Know About Delta Lake 4.1
# MAGIC
# MAGIC **Article:** [What Developers Need to Know About Delta Lake 4.1](https://medium.com/@cralle/what-developers-need-to-know-about-delta-lake-4-1-f558abf85b10?sk=7326303f9ebc180b7e9dc7a781b61545)
# MAGIC
# MAGIC **Author:** Christian Hansen (https://medium.com/@cralle)
# MAGIC
# MAGIC **Published:** Apr 1, 2026
# MAGIC
# MAGIC A tour of Delta Lake 4.1.0: Apache Spark 4.1.0 support and the new versioned artifact naming, catalog-managed tables (still in preview), atomic CREATE TABLE AS SELECT, Server-Side Planning (preview), conflict-free enablement of Deletion Vectors and Column Mapping, AWS storage credentials and external locations, and the Delta Kernel / new V2 connector additions, each demonstrated against small CH Enterprise sample tables.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Setup
# MAGIC
# MAGIC Creates the `testing.default` catalog/schema (if they do not already exist) and a single foundational CH Enterprise orders table, used across the Atomic CTAS, Server-Side Planning, and Conflict-Free Feature Enablement sections below. The Catalog-Managed Tables section creates its own self-contained demo table further down, since it needs specific reader/writer protocol versions from the moment it is created.

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE CATALOG IF NOT EXISTS testing;
# MAGIC CREATE SCHEMA IF NOT EXISTS testing.default;
# MAGIC USE CATALOG testing;
# MAGIC USE SCHEMA default;

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE testing.default.ch_enterprise_orders (
# MAGIC   order_id BIGINT,
# MAGIC   customer_id INT,
# MAGIC   order_amount DECIMAL(10,2),
# MAGIC   order_date DATE
# MAGIC );
# MAGIC
# MAGIC INSERT INTO testing.default.ch_enterprise_orders VALUES
# MAGIC   (5001, 1001, 4820.50, DATE'2026-07-01'),
# MAGIC   (5002, 1002, 990.00,  DATE'2026-07-03'),
# MAGIC   (5003, 1001, 15200.75, DATE'2026-07-10'),
# MAGIC   (5004, 1003, 610.25,  DATE'2026-07-12'),
# MAGIC   (5005, 1002, 2330.00, DATE'2026-07-18');

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Apache Spark 4.1.0 Support and New Artifact Naming
# MAGIC
# MAGIC Delta Lake 4.1.0 adds full support for Apache Spark 4.1.0 while remaining compatible with Spark 4.0.1. Starting with this release, Maven artifacts include a Spark version suffix to make the pairing explicit, for example `io.delta:delta-spark_4.1_2.13:4.1.0`, alongside PyPI's `pip install delta-spark==4.1.0`. Backward-compatible artifacts without the suffix are still published for now, but the recommendation is to move to the versioned naming format going forward.
# MAGIC
# MAGIC **Breaking changes to be aware of:**
# MAGIC - Java 17 or higher is now required.
# MAGIC - Spark 3.5 support has been officially dropped; you need to be on Spark 4.0.1 or 4.1.0.
# MAGIC - Manual `VACUUM` is blocked for catalog-managed tables; data lifecycle for those tables is managed through the catalog instead (see the Catalog-Managed Tables section below, and note this in Cleanup: the demo catalog-managed table there is removed with `DROP TABLE`, not `VACUUM`).
# MAGIC
# MAGIC The cell below just confirms the Spark runtime this notebook is actually running on, since everything after this depends on being on Spark 4.0.1+ with Delta Lake 4.1.0.

# COMMAND ----------

print(f"Spark version: {spark.version}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Catalog-Managed Tables (Preview)
# MAGIC
# MAGIC Catalog-managed tables shift the source of truth for table state away from the filesystem and onto the catalog, which becomes the coordinator of table access. This simplifies how tables are discovered, secured, and governed across different compute engines. The feature now has full support in both Delta Spark and Delta Kernel.
# MAGIC
# MAGIC **Preview caveat:** catalog-managed tables are still in preview and are not recommended for production usage; the protocol is still evolving and behavior may change in future releases. The feature is supported and active on a table when it is on Reader Version 3 and Writer Version 7, with a protocol action whose `readerFeatures` and `writerFeatures` both contain `catalogManaged`. Whether this succeeds depends on your Unity Catalog metastore having the preview enabled; if it is not, the `CREATE TABLE` below will fail with an unsupported-feature error, which is expected given the preview status.

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE testing.default.ch_enterprise_catalog_managed_orders (
# MAGIC   order_id BIGINT,
# MAGIC   customer_id INT,
# MAGIC   order_amount DECIMAL(10,2),
# MAGIC   order_date DATE
# MAGIC )
# MAGIC USING DELTA
# MAGIC TBLPROPERTIES ('delta.minReaderVersion' = '3', 'delta.minWriterVersion' = '7');
# MAGIC
# MAGIC INSERT INTO testing.default.ch_enterprise_catalog_managed_orders VALUES
# MAGIC   (6001, 1001, 3120.00, DATE'2026-07-05'),
# MAGIC   (6002, 1004, 480.00,  DATE'2026-07-09');

# COMMAND ----------

# MAGIC %md
# MAGIC Once created, reads, writes, and history inspection all flow through the catalog as the single source of truth. The cells below show a batch read, a streaming read (bounded with `trigger(availableNow=True)` so it runs to completion inside this notebook cell), and `DESCRIBE HISTORY`.

# COMMAND ----------

catalog_managed_orders_df = spark.table("testing.default.ch_enterprise_catalog_managed_orders")
catalog_managed_orders_df.orderBy("order_id").display()

# COMMAND ----------

stream_df = spark.readStream.format("delta").table("testing.default.ch_enterprise_catalog_managed_orders")

stream_query = (
    stream_df.writeStream
    .trigger(availableNow=True)
    .format("memory")
    .queryName("ch_enterprise_catalog_managed_stream")
    .start()
)
stream_query.awaitTermination()

spark.sql("SELECT * FROM ch_enterprise_catalog_managed_stream ORDER BY order_id").display()

# COMMAND ----------

# MAGIC %sql
# MAGIC DESCRIBE HISTORY testing.default.ch_enterprise_catalog_managed_orders;

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Atomic CTAS
# MAGIC
# MAGIC `CREATE TABLE AS SELECT` operations are now fully atomic for Delta tables when running with Unity Catalog 0.4.0. Previously, a failure mid-write could leave a partially written or corrupted table behind, requiring manual cleanup before retrying; a standard CTAS now either completes fully or rolls back cleanly with no intermediate state left on storage. This also composes cleanly with `CREATE OR REPLACE TABLE AS SELECT` for idempotent pipeline patterns, which is particularly useful in scheduled pipelines where you want a clean, atomic refresh of a derived table on every run.
# MAGIC
# MAGIC The cell below builds a summary table from `ch_enterprise_orders`, then re-runs the same statement with `CREATE OR REPLACE` to show the idempotent refresh pattern: you can safely retry or re-run it without worrying about cleaning up a partial table first.

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE IF NOT EXISTS testing.default.ch_enterprise_customers_summary
# MAGIC USING DELTA AS
# MAGIC SELECT
# MAGIC   customer_id,
# MAGIC   COUNT(order_id) AS total_orders,
# MAGIC   SUM(order_amount) AS total_spend,
# MAGIC   MAX(order_date) AS last_order_date
# MAGIC FROM testing.default.ch_enterprise_orders
# MAGIC GROUP BY customer_id;
# MAGIC
# MAGIC SELECT * FROM testing.default.ch_enterprise_customers_summary ORDER BY customer_id;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Idempotent, atomic refresh: safe to re-run on every scheduled pipeline execution
# MAGIC CREATE OR REPLACE TABLE testing.default.ch_enterprise_customers_summary
# MAGIC USING DELTA AS
# MAGIC SELECT
# MAGIC   customer_id,
# MAGIC   COUNT(order_id) AS total_orders,
# MAGIC   SUM(order_amount) AS total_spend,
# MAGIC   MAX(order_date) AS last_order_date
# MAGIC FROM testing.default.ch_enterprise_orders
# MAGIC GROUP BY customer_id;
# MAGIC
# MAGIC SELECT * FROM testing.default.ch_enterprise_customers_summary ORDER BY customer_id;

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Server-Side Planning (Preview)
# MAGIC
# MAGIC Server-Side Planning lets Delta Lake's Spark connector delegate table scan planning to external catalog services, such as Unity Catalog, rather than executing it locally on the driver. File discovery, predicate filtering, and credential provisioning all happen server-side via Unity Catalog, following the Iceberg REST Catalog protocol. The driver never receives raw storage credentials, which is the key enabler for Fine-Grained Access Control: for tables with row-level or column-level access policies, the catalog decides exactly which files a query is allowed to see and returns scoped, temporary credentials accordingly. It also brings performance benefits, since server-side pushdown of filters, projections, and limits means significantly less metadata needs to flow to the client before query execution begins.
# MAGIC
# MAGIC **Preview caveat:** this is a preview feature and there is no separate per-query toggle in the SQL or PySpark API; it activates transparently for eligible tables once your Unity Catalog metastore supports it. There is nothing to flip on from a notebook cell, so the demo below just runs the kind of filter-projection-limit query that benefits from the pushdown, and inspects the plan.

# COMMAND ----------

import pyspark.sql.functions as F

filtered_orders_df = (
    spark.table("testing.default.ch_enterprise_orders")
    .filter(F.col("order_amount") > 1000)
    .select("order_id", "customer_id", "order_amount")
    .limit(10)
)

filtered_orders_df.explain(mode="formatted")
filtered_orders_df.orderBy("order_id").display()

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5. Conflict-Free Feature Enablement
# MAGIC
# MAGIC You can now enable Deletion Vectors and Column Mapping on existing tables without scheduling a maintenance window or blocking concurrent writes. Concurrent reads and writes continue uninterrupted while the feature is being enabled, which removes one of the more painful operational constraints when rolling out these features on active tables. (Simulating actual concurrent writers needs multiple sessions and is not shown here; the `ALTER TABLE` statements below are exactly what is now safe to run against a table that is being read and written concurrently.)

# COMMAND ----------

# MAGIC %sql
# MAGIC ALTER TABLE testing.default.ch_enterprise_orders
# MAGIC SET TBLPROPERTIES ('delta.enableDeletionVectors' = 'true');

# COMMAND ----------

# MAGIC %sql
# MAGIC ALTER TABLE testing.default.ch_enterprise_orders
# MAGIC SET TBLPROPERTIES (
# MAGIC   'delta.columnMapping.mode' = 'name',
# MAGIC   'delta.minReaderVersion' = '2',
# MAGIC   'delta.minWriterVersion' = '5'
# MAGIC );

# COMMAND ----------

# MAGIC %sql
# MAGIC SHOW TBLPROPERTIES testing.default.ch_enterprise_orders;

# COMMAND ----------

# MAGIC %md
# MAGIC ## 6. AWS Storage Credentials and External Locations
# MAGIC
# MAGIC Delta Lake 4.1.0 introduces first-class resource management for AWS IAM roles and S3 storage in Unity Catalog, bringing AWS in line with the storage management capabilities already available on Azure.
# MAGIC
# MAGIC **Manual setup required:** creating a real storage credential needs an AWS IAM role that already trusts Databricks and has access to a real S3 bucket, plus metastore admin (or `CREATE STORAGE CREDENTIAL`/`CREATE EXTERNAL LOCATION`) privileges. Neither of those exists in a fresh workspace or account by default, and cannot be provisioned from inside a notebook cell. The SQL below is shown for reference only, with placeholder values, and is not executed against this workspace.
# MAGIC
# MAGIC ```sql
# MAGIC -- Reference only - requires a real AWS IAM role trust relationship and an existing S3 bucket
# MAGIC CREATE STORAGE CREDENTIAL ch_enterprise_aws_storage_credential
# MAGIC WITH (
# MAGIC   AWS_IAM_ROLE = 'arn:aws:iam::<YOUR_ACCOUNT_ID>:role/ch-enterprise-uc-access'
# MAGIC );
# MAGIC
# MAGIC CREATE EXTERNAL LOCATION ch_enterprise_s3_data_lake
# MAGIC URL 's3://<YOUR_BUCKET_NAME>/ch-enterprise-data-lake/'
# MAGIC WITH (STORAGE CREDENTIAL ch_enterprise_aws_storage_credential);
# MAGIC ```
# MAGIC
# MAGIC The read-only verification statements below work in any Unity Catalog workspace and are safe to run as is, since they only list objects that already exist (or return an empty result if none do).

# COMMAND ----------

# MAGIC %sql
# MAGIC SHOW STORAGE CREDENTIALS;

# COMMAND ----------

# MAGIC %sql
# MAGIC SHOW EXTERNAL LOCATIONS;

# COMMAND ----------

# MAGIC %md
# MAGIC ## 7. Additional Features: Delta Kernel and the New V2 Connector
# MAGIC
# MAGIC Two smaller additions round out this release:
# MAGIC - **Delta Kernel support.** Kernel-based connectors can now interact with catalog-managed tables directly, with commit coordination handled by the catalog rather than the filesystem.
# MAGIC - **New V2 Connector.** A Spark DataSource V2 connector backed by Delta Kernel is now available, with initial support for streaming reads on catalog-managed tables. This is what powers the streaming read against `ch_enterprise_catalog_managed_orders` already demonstrated in section 2 above; there is nothing further to run for it here.
# MAGIC
# MAGIC **Caveat:** Delta Kernel itself is a Java/Rust library aimed at people building connectors, not at Spark or PySpark users. It exposes narrow Table APIs (`Table`, `Snapshot`) and pluggable Engine APIs, but there is no PySpark or SQL entry point to call from a Databricks notebook, so there is no runnable demo for it here. The shape of a Kernel-based scan is shown for reference only:
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
# MAGIC ## Cleanup
# MAGIC
# MAGIC Drops every table created during Setup and the numbered sections above. The catalog-managed table is removed with `DROP TABLE`, in line with the article's note that manual `VACUUM` is now blocked on catalog-managed tables; `DROP TABLE` is unaffected by that restriction. The bounded memory-format streaming query from section 2 already finished (via `awaitTermination()`), so it does not need to be stopped separately, but its temporary view is dropped for good measure. No storage credential or external location was created in section 6, since that part was reference only.

# COMMAND ----------

spark.catalog.dropTempView("ch_enterprise_catalog_managed_stream")

# COMMAND ----------

# MAGIC %sql
# MAGIC DROP TABLE IF EXISTS testing.default.ch_enterprise_customers_summary;
# MAGIC DROP TABLE IF EXISTS testing.default.ch_enterprise_catalog_managed_orders;
# MAGIC DROP TABLE IF EXISTS testing.default.ch_enterprise_orders;

# COMMAND ----------

# MAGIC %md
# MAGIC **Notes:**
# MAGIC - Delta Lake 4.1.0 was released February 20, 2026. Catalog-Managed Tables and Server-Side Planning are both explicitly called out in the article as preview features, not recommended for production; treat any failures on those two cells as workspace/preview-enablement differences, not bugs in the notebook.
# MAGIC - Section 6 (AWS Storage Credentials and External Locations) is reference-only: creating a real storage credential needs an AWS IAM role and an S3 bucket that already exist outside of Databricks, plus metastore admin privileges, none of which can be provisioned from a notebook cell.
# MAGIC - Delta Kernel (section 7) is a Java/Rust library with no PySpark or SQL entry point, so it is shown as reference code only, matching how it was handled in the companion Delta Lake 4.0 notebook.
