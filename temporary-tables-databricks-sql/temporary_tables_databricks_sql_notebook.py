# Databricks notebook source
# MAGIC %md
# MAGIC # Temporary Tables in Databricks SQL: Staging Data Without Cluttering Your Catalog
# MAGIC
# MAGIC **Article:** [Temporary Tables in Databricks SQL: Staging Data Without Cluttering Your Catalog](https://medium.com/@cralle/temporary-tables-in-databricks-sql-a-familiar-pattern-finally-done-right-a5dcee1609a4?sk=65307b456bebc39155b04f7b05e658d8)
# MAGIC
# MAGIC Author: Christian Hansen (https://medium.com/@cralle) | Published: January 30, 2026
# MAGIC
# MAGIC Databricks SQL now supports `CREATE TEMPORARY TABLE` (Public Preview): session-scoped,
# MAGIC physical Delta tables for staging, multi-step ETL, and exploratory SQL, without ever
# MAGIC registering an object in Unity Catalog.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Overview
# MAGIC
# MAGIC Temporary tables are session-scoped, physical tables that store data for the lifetime
# MAGIC of a Databricks SQL session, up to a maximum of seven days. Unlike a temporary view,
# MAGIC a temporary table actually materializes its rows as a Delta table behind the scenes,
# MAGIC so it can be queried repeatedly, appended to with `INSERT`, and joined against, without
# MAGIC re-running the query that built it every time. Unlike a permanent table, it is never
# MAGIC registered in Unity Catalog, is invisible outside the session that created it, and is
# MAGIC cleaned up automatically when that session ends.
# MAGIC
# MAGIC This notebook:
# MAGIC
# MAGIC 1. Creates two small CH Enterprise sample tables to stage from (**Setup**).
# MAGIC 2. Creates and uses a temporary table (creation, querying, `INSERT`, chaining downstream).
# MAGIC 3. Demonstrates session scope and isolation, including name precedence over a permanent
# MAGIC    table, and is explicit about what does and does not carry over to a new session.
# MAGIC 4. Compares temporary tables to temporary views, CTEs, and permanent tables side by side.
# MAGIC 5. Runs a realistic multi-step ETL pipeline: stage, clean, aggregate, then `MERGE INTO`
# MAGIC    a permanent production table.
# MAGIC 6. Lists the current Public Preview limitations, shown as reference syntax rather than
# MAGIC    executed, since they are expected to fail by design.
# MAGIC 7. Cleans up every object this notebook created (**Cleanup**).
# MAGIC
# MAGIC **Compute note:** `CREATE TEMPORARY TABLE` is a Databricks SQL feature. Run this
# MAGIC notebook against a SQL warehouse (or serverless SQL compute); the limitations section
# MAGIC below notes that the feature does not currently support the classic DataFrame/Spark API.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Setup
# MAGIC
# MAGIC Create the sandbox catalog and schema (`testing.default`) and two permanent sample
# MAGIC tables that stand in for CH Enterprise data: recent sales transactions and the customers
# MAGIC behind them. Sample dates are generated relative to `current_date()` so the "last 7 days"
# MAGIC filters used throughout this notebook always return a realistic mix of rows, regardless
# MAGIC of when the notebook is actually run.

# COMMAND ----------

catalog = "testing"
schema = "default"

spark.sql(f"CREATE CATALOG IF NOT EXISTS {catalog}")
spark.sql(f"CREATE SCHEMA IF NOT EXISTS {catalog}.{schema}")
spark.sql(f"USE CATALOG {catalog}")
spark.sql(f"USE SCHEMA {schema}")

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Raw CH Enterprise sales transactions. days_ago is used only to derive sale_date
# MAGIC -- relative to today, so the table always contains both recent and older rows.
# MAGIC CREATE OR REPLACE TABLE testing.default.ch_raw_sales AS
# MAGIC SELECT
# MAGIC   sale_id,
# MAGIC   date_sub(current_date(), days_ago) AS sale_date,
# MAGIC   region,
# MAGIC   customer_id,
# MAGIC   amount
# MAGIC FROM VALUES
# MAGIC   (1, 1, 'West', 101, 1200.50),
# MAGIC   (2, 2, 'East', 102, 875.00),
# MAGIC   (3, 3, 'West', 103, 430.25),
# MAGIC   (4, 9, 'North', 104, 2100.00),
# MAGIC   (5, 12, 'South', 105, 560.75),
# MAGIC   (6, 5, 'East', 101, 990.00),
# MAGIC   (7, 6, 'North', 106, 340.10),
# MAGIC   (8, 15, 'West', 107, 1875.60)
# MAGIC   AS t(sale_id, days_ago, region, customer_id, amount);
# MAGIC
# MAGIC SELECT * FROM testing.default.ch_raw_sales ORDER BY sale_date;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- CH Enterprise customers referenced by the sales above.
# MAGIC CREATE OR REPLACE TABLE testing.default.ch_customers AS
# MAGIC SELECT * FROM VALUES
# MAGIC   (101, 'Nordic Retail Group', 'Gold'),
# MAGIC   (102, 'Baltic Manufacturing', 'Silver'),
# MAGIC   (103, 'Fjord Logistics', 'Gold'),
# MAGIC   (104, 'Alpine Foods', 'Platinum'),
# MAGIC   (105, 'Harbor Textiles', 'Silver'),
# MAGIC   (106, 'Summit Electronics', 'Bronze'),
# MAGIC   (107, 'Northern Freight Co', 'Gold')
# MAGIC   AS t(customer_id, customer_name, customer_tier);
# MAGIC
# MAGIC SELECT * FROM testing.default.ch_customers ORDER BY customer_id;

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Creating and Using a Temporary Table
# MAGIC
# MAGIC `CREATE TEMPORARY TABLE ... AS SELECT` behaves like a normal `CTAS`, except the result
# MAGIC is never written into Unity Catalog. The table can be queried with plain `SELECT`,
# MAGIC appended to with `INSERT`, and used as the source for another temporary table, exactly
# MAGIC like the article's `tmp_sales` / `tmp_enriched` example.

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TEMPORARY TABLE tmp_sales AS
# MAGIC SELECT *
# MAGIC FROM testing.default.ch_raw_sales
# MAGIC WHERE sale_date >= current_date() - INTERVAL 7 DAYS;
# MAGIC
# MAGIC SELECT * FROM tmp_sales ORDER BY sale_date;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Query it as many times as needed; no recomputation of the WHERE filter above happens here.
# MAGIC SELECT region, SUM(amount) AS total_amount
# MAGIC FROM tmp_sales
# MAGIC GROUP BY region
# MAGIC ORDER BY total_amount DESC;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Modify it: new sales data arrives and gets appended without rebuilding tmp_sales.
# MAGIC CREATE OR REPLACE TEMPORARY TABLE tmp_new_sales AS
# MAGIC SELECT * FROM VALUES
# MAGIC   (9, current_date(), 'East', 108, 725.40),
# MAGIC   (10, current_date(), 'West', 101, 610.00)
# MAGIC   AS t(sale_id, sale_date, region, customer_id, amount);
# MAGIC
# MAGIC INSERT INTO tmp_sales
# MAGIC SELECT * FROM tmp_new_sales;
# MAGIC
# MAGIC SELECT count(*) AS row_count FROM tmp_sales;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Use it downstream: build a second temporary table on top of the first, joined
# MAGIC -- against a permanent Unity Catalog table.
# MAGIC CREATE OR REPLACE TEMPORARY TABLE tmp_enriched AS
# MAGIC SELECT s.*, c.customer_name, c.customer_tier
# MAGIC FROM tmp_sales s
# MAGIC JOIN testing.default.ch_customers c
# MAGIC   ON s.customer_id = c.customer_id;
# MAGIC
# MAGIC SELECT * FROM tmp_enriched ORDER BY sale_date;

# COMMAND ----------

# MAGIC %md
# MAGIC None of this created anything in `testing.default`. Confirm with `SHOW TABLES`,
# MAGIC which only lists the two permanent tables created in Setup, not `tmp_sales`,
# MAGIC `tmp_new_sales`, or `tmp_enriched`.

# COMMAND ----------

# MAGIC %sql
# MAGIC SHOW TABLES IN testing.default;

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Session Scope and Isolation
# MAGIC
# MAGIC Temporary tables are isolated to the single session that created them: invisible to
# MAGIC other users, and safe from naming conflicts with permanent tables. If a temporary table
# MAGIC shares a name with a permanent one, the temporary table takes precedence for the
# MAGIC unqualified name within that session; the permanent table is still reachable with its
# MAGIC fully qualified name.
# MAGIC
# MAGIC The cell below creates a permanent table and a temporary table with the same short name
# MAGIC to show that precedence directly.

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE testing.default.tmp_conflict_demo AS
# MAGIC SELECT 'permanent table' AS source;
# MAGIC
# MAGIC CREATE OR REPLACE TEMPORARY TABLE tmp_conflict_demo AS
# MAGIC SELECT 'temporary table' AS source;
# MAGIC
# MAGIC -- Unqualified name: the temporary table wins within this session
# MAGIC SELECT * FROM tmp_conflict_demo;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Fully qualified name: the permanent table is still reachable directly
# MAGIC SELECT * FROM testing.default.tmp_conflict_demo;

# COMMAND ----------

# MAGIC %md
# MAGIC **Important caveat on scope, do not assume more persistence than actually exists:**
# MAGIC `tmp_sales`, `tmp_new_sales`, `tmp_enriched`, and `tmp_conflict_demo` only exist for as
# MAGIC long as the current SQL session lives (up to seven days, less if the session ends
# MAGIC sooner). They are visible across cells in *this* notebook only because those cells share
# MAGIC one session while it stays attached to the same SQL warehouse. They will **not** be
# MAGIC visible:
# MAGIC
# MAGIC - From a different notebook, even one attached to the same SQL warehouse, since it opens
# MAGIC   its own session.
# MAGIC - From a different user's session against the same warehouse.
# MAGIC - From this same notebook after the warehouse restarts, the session times out, or the
# MAGIC   notebook is detached and reattached, since that starts a new session too.
# MAGIC
# MAGIC In other words, re-running only the Cleanup cells in a brand-new session will find
# MAGIC nothing to drop for the temporary tables, they will already be gone; only the permanent
# MAGIC objects created in Setup and Section 6 persist across sessions.

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Temporary Tables vs Views, CTEs, and Permanent Tables
# MAGIC
# MAGIC | | Temporary view | CTE | Temporary table | Permanent table |
# MAGIC |---|---|---|---|---|
# MAGIC | Stores data | No, logical only | No, logical only | Yes, physical Delta | Yes, physical Delta |
# MAGIC | Re-evaluates on each reference | Yes | Yes (per statement) | No, materialized once | No, materialized once |
# MAGIC | Scope | Session | Single statement | Session | Unity Catalog namespace (permanent) |
# MAGIC | Supports `INSERT` / `UPDATE` / `MERGE` | No | No | Yes | Yes |
# MAGIC | Registered in Unity Catalog | No | No | No | Yes |
# MAGIC
# MAGIC The cell below runs the same "recent sales by region" logic three ways to make the
# MAGIC distinction concrete.

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Temporary view: logical only. Each SELECT below re-runs the WHERE + GROUP BY.
# MAGIC CREATE OR REPLACE TEMPORARY VIEW v_sales_by_region AS
# MAGIC SELECT region, SUM(amount) AS total_amount
# MAGIC FROM testing.default.ch_raw_sales
# MAGIC WHERE sale_date >= current_date() - INTERVAL 7 DAYS
# MAGIC GROUP BY region;
# MAGIC
# MAGIC SELECT * FROM v_sales_by_region ORDER BY total_amount DESC;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- CTE: scoped to this single statement only, cannot be referenced from a later cell at all.
# MAGIC WITH recent_sales AS (
# MAGIC   SELECT * FROM testing.default.ch_raw_sales
# MAGIC   WHERE sale_date >= current_date() - INTERVAL 7 DAYS
# MAGIC )
# MAGIC SELECT region, SUM(amount) AS total_amount
# MAGIC FROM recent_sales
# MAGIC GROUP BY region
# MAGIC ORDER BY total_amount DESC;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Temporary table: materialized once back in Section 1 as tmp_sales. Querying it again
# MAGIC -- here reuses those stored rows instead of re-running the original filter.
# MAGIC SELECT region, SUM(amount) AS total_amount
# MAGIC FROM tmp_sales
# MAGIC GROUP BY region
# MAGIC ORDER BY total_amount DESC;

# COMMAND ----------

# MAGIC %md
# MAGIC If `v_sales_by_region` were queried in a loop or referenced from several downstream
# MAGIC queries, the underlying scan and aggregation would re-run every single time. `tmp_sales`
# MAGIC already paid that cost once in Section 1; every query against it since has just been
# MAGIC reading stored Delta rows. For a result that is reused more than once or two, the
# MAGIC temporary table is the cheaper choice.

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Multi-Step ETL with Temporary Tables
# MAGIC
# MAGIC A common shape for SQL-driven ETL is stage, clean, aggregate, then merge into a
# MAGIC production table. Each step used to mean either an expensive re-run through a view, a
# MAGIC permanent staging table cluttering the catalog, or dropping into the DataFrame API.
# MAGIC Temporary tables let every intermediate step stay in plain, readable SQL.

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Step 1: stage the raw data
# MAGIC CREATE OR REPLACE TEMPORARY TABLE tmp_stage AS
# MAGIC SELECT * FROM testing.default.ch_raw_sales;
# MAGIC
# MAGIC -- Step 2: clean and normalize
# MAGIC CREATE OR REPLACE TEMPORARY TABLE tmp_clean AS
# MAGIC SELECT sale_id, sale_date, upper(region) AS region, customer_id, amount
# MAGIC FROM tmp_stage
# MAGIC WHERE amount > 0;
# MAGIC
# MAGIC -- Step 3: aggregate
# MAGIC CREATE OR REPLACE TEMPORARY TABLE tmp_agg AS
# MAGIC SELECT region, SUM(amount) AS total_amount, current_timestamp() AS updated_at
# MAGIC FROM tmp_clean
# MAGIC GROUP BY region;
# MAGIC
# MAGIC SELECT * FROM tmp_agg ORDER BY region;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Step 4: merge the aggregated result into a permanent production table
# MAGIC CREATE TABLE IF NOT EXISTS testing.default.ch_sales_summary (
# MAGIC   region STRING,
# MAGIC   total_amount DECIMAL(12,2),
# MAGIC   updated_at TIMESTAMP
# MAGIC );
# MAGIC
# MAGIC MERGE INTO testing.default.ch_sales_summary AS target
# MAGIC USING tmp_agg AS source
# MAGIC ON target.region = source.region
# MAGIC WHEN MATCHED THEN
# MAGIC   UPDATE SET target.total_amount = source.total_amount, target.updated_at = source.updated_at
# MAGIC WHEN NOT MATCHED THEN
# MAGIC   INSERT (region, total_amount, updated_at)
# MAGIC   VALUES (source.region, source.total_amount, source.updated_at);
# MAGIC
# MAGIC SELECT * FROM testing.default.ch_sales_summary ORDER BY region;

# COMMAND ----------

import pyspark.sql.functions as F

# The permanent result of the ETL pipeline above can be read back like any other table.
# This does not touch the temporary tables themselves, they are SQL-only for now (see
# Section 5); it simply reads the permanent table that the MERGE INTO wrote to.
summary_df = spark.table("testing.default.ch_sales_summary")
(
    summary_df
    .select("region", F.round(F.col("total_amount"), 2).alias("total_amount"), "updated_at")
    .orderBy("region")
    .display()
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5. Current Limitations (Public Preview)
# MAGIC
# MAGIC This feature is in Public Preview, and the article calls out a specific set of
# MAGIC constraints. Each snippet below is genuinely expected to fail under these limitations,
# MAGIC so it is shown as reference syntax rather than being forced to run in this notebook:
# MAGIC
# MAGIC - **SQL only.** No DataFrame or Spark API support for creating or managing temporary
# MAGIC   tables directly (`spark.sql("CREATE TEMPORARY TABLE ...")` works because it is still
# MAGIC   just sending SQL text to the engine; there is no `df.createTemporaryTable(...)`
# MAGIC   equivalent on the DataFrame API itself).
# MAGIC - **No `ALTER TABLE` support yet.**
# MAGIC   ```sql
# MAGIC   ALTER TABLE tmp_sales ADD COLUMN discount DECIMAL(10,2);
# MAGIC   ```
# MAGIC - **No time travel.**
# MAGIC   ```sql
# MAGIC   SELECT * FROM tmp_sales VERSION AS OF 1;
# MAGIC   ```
# MAGIC - **No streaming usage** as a source or sink.
# MAGIC - **No `DELETE` operations.**
# MAGIC   ```sql
# MAGIC   DELETE FROM tmp_sales WHERE amount < 100;
# MAGIC   ```
# MAGIC - **Cannot be used in multi-user notebook sessions**, where a session is shared across
# MAGIC   more than one user.
# MAGIC
# MAGIC These are constraints of the Public Preview, not permanent design limits, and they do
# MAGIC not diminish the core value of temporary tables for SQL-driven staging and ETL.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Cleanup
# MAGIC
# MAGIC Explicit `DROP TABLE` on the temporary tables is optional since they disappear on their
# MAGIC own when the session ends (or after seven days, whichever comes first), but dropping
# MAGIC them here keeps this notebook's own session tidy while iterating. The permanent objects
# MAGIC created in Setup and Section 4 are dropped too, since they are real Unity Catalog tables
# MAGIC that would otherwise stick around.

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Temporary tables must be dropped with DROP TEMPORARY TABLE. A plain
# MAGIC -- DROP TABLE with an unqualified name resolves to a permanent table and
# MAGIC -- errors when a temporary table of the same name exists in the session
# MAGIC -- (for example tmp_conflict_demo, which also exists as a permanent table).
# MAGIC DROP VIEW IF EXISTS v_sales_by_region;
# MAGIC DROP TEMPORARY TABLE IF EXISTS tmp_enriched;
# MAGIC DROP TEMPORARY TABLE IF EXISTS tmp_new_sales;
# MAGIC DROP TEMPORARY TABLE IF EXISTS tmp_sales;
# MAGIC DROP TEMPORARY TABLE IF EXISTS tmp_conflict_demo;
# MAGIC DROP TEMPORARY TABLE IF EXISTS tmp_stage;
# MAGIC DROP TEMPORARY TABLE IF EXISTS tmp_clean;
# MAGIC DROP TEMPORARY TABLE IF EXISTS tmp_agg;

# COMMAND ----------

spark.sql("DROP TABLE IF EXISTS testing.default.tmp_conflict_demo")
spark.sql("DROP TABLE IF EXISTS testing.default.ch_sales_summary")
spark.sql("DROP TABLE IF EXISTS testing.default.ch_raw_sales")
spark.sql("DROP TABLE IF EXISTS testing.default.ch_customers")

print("Temporary tables dropped for this session, and all permanent sample/demo tables removed.")

# COMMAND ----------

# MAGIC %md
# MAGIC **Notes:** `CREATE TEMPORARY TABLE` was in Public Preview at the time of writing, and is
# MAGIC a Databricks SQL feature, run this notebook against a SQL warehouse or serverless SQL
# MAGIC compute. Temporary tables are strictly session-scoped: they are visible across the cells
# MAGIC of this notebook only because those cells share one attached session, they are never
# MAGIC visible from a different notebook, a different user, or the same notebook after its
# MAGIC session restarts, and they disappear automatically after the session ends or after seven
# MAGIC days, whichever comes first.
