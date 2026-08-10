# Databricks notebook source
# MAGIC %md
# MAGIC **Article:** [Databricks Predictive Optimization: How Automatic OPTIMIZE, VACUUM, and CLUSTER BY AUTO Really Work](https://medium.com/@cralle/demystifying-predictive-optimization-in-databricks-automate-table-tuning-with-confidence-a5bc293292c3?sk=026c78ada316bfc19b45f7f8f555b9bc)
# MAGIC
# MAGIC # Databricks Predictive Optimization: How Automatic OPTIMIZE, VACUUM, and CLUSTER BY AUTO Really Work
# MAGIC
# MAGIC Author: Christian Hansen (https://medium.com/@cralle)
# MAGIC
# MAGIC A walkthrough of how Predictive Optimization decides when to run OPTIMIZE, VACUUM,
# MAGIC and ANALYZE on Unity Catalog managed tables, how CLUSTER BY AUTO (Automatic Liquid
# MAGIC Clustering) picks and re-evaluates clustering columns, and how to monitor both
# MAGIC through system tables, using CH Enterprise sample tables in testing.default.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Overview
# MAGIC
# MAGIC Predictive Optimization is a background service on Unity Catalog managed tables that
# MAGIC continuously decides, on its own, whether a table would benefit from `OPTIMIZE`,
# MAGIC `ANALYZE`, `VACUUM`, or (via `CLUSTER BY AUTO`) a change to its clustering columns. It
# MAGIC does not run inside a notebook cell and it does not run synchronously, it is scheduled
# MAGIC by Databricks based on a return-on-investment (ROI) estimate: table usage, the size of
# MAGIC files being scanned, the estimated query speedup, and the estimated cost of running the
# MAGIC operation. Since May 7th, 2025, it has been enabled by default for every existing
# MAGIC Databricks account.
# MAGIC
# MAGIC Because the actual background execution is asynchronous and account-scoped, this
# MAGIC notebook cannot make Predictive Optimization run and observe the result in the same
# MAGIC session. What it does instead, and what is genuinely runnable end to end:
# MAGIC
# MAGIC 1. Create CH Enterprise sample tables with a file layout that OPTIMIZE/clustering would
# MAGIC    meaningfully act on.
# MAGIC 2. Set the same `ENABLE` / `DISABLE` / `INHERIT` state and `CLUSTER BY AUTO` settings
# MAGIC    Predictive Optimization reads, at catalog, schema, and table level.
# MAGIC 3. Run the manual equivalents (`OPTIMIZE`, `ANALYZE`, `VACUUM`) for comparison.
# MAGIC 4. Query the system tables Predictive Optimization writes its own decisions and billing
# MAGIC    into, so the same monitoring queries work once the account has enough history.
# MAGIC
# MAGIC The one piece that cannot be done from SQL/PySpark at all, turning Predictive
# MAGIC Optimization on at the account level if it is off, is called out in **Manual setup
# MAGIC required** below.
# MAGIC
# MAGIC Source for the mechanics described here: a DAIS 2025 session by Cindy Jiang and Naga
# MAGIC Bhanoori ("Data Intelligence on Unity Catalog Managed Tables Powered by Predictive
# MAGIC Optimization"), plus Databricks documentation. Databricks does not publish the exact
# MAGIC scoring algorithm, so treat the ROI factors below as a directional explanation, not an
# MAGIC exact formula.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Setup
# MAGIC
# MAGIC Creates two CH Enterprise sample tables in `testing.default`:
# MAGIC
# MAGIC - `ch_enterprise_orders`: an orders table written out as roughly 40 small files on
# MAGIC   purpose (by repartitioning before the write), the kind of layout `OPTIMIZE` /
# MAGIC   Predictive Optimization is meant to compact.
# MAGIC - `ch_enterprise_events`: a clickstream-style events table used for the
# MAGIC   `CLUSTER BY AUTO` section, with a `region` and `event_type` column that would make
# MAGIC   reasonable clustering key candidates for a filter-heavy workload.
# MAGIC
# MAGIC Both are ordinary Unity Catalog managed Delta tables, no special setup is needed to
# MAGIC create them.

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE SCHEMA IF NOT EXISTS testing.default;

# COMMAND ----------

import pyspark.sql.functions as F

row_count = 20000

orders = (
    spark.range(0, row_count)
    .withColumn("order_id", F.concat(F.lit("ORD-"), F.col("id").cast("string")))
    .withColumn("customer_id", (F.col("id") % 500).cast("int"))
    .withColumn(
        "region",
        F.element_at(
            F.array(F.lit("EMEA"), F.lit("AMER"), F.lit("APAC")),
            (F.col("id") % 3 + 1).cast("int"),
        ),
    )
    .withColumn("order_total", F.round(F.rand(seed=42) * 500, 2))
    .withColumn("order_date", F.expr("date'2025-01-01' + cast(id % 180 as int)"))
    .drop("id")
)

# Repartition into many small files on purpose. This is the file layout OPTIMIZE
# (run manually or by Predictive Optimization) is meant to compact.
(
    orders.repartition(40)
    .write.format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable("testing.default.ch_enterprise_orders")
)

print(f"testing.default.ch_enterprise_orders created with {row_count} rows.")

# COMMAND ----------

event_count = 50000

events = (
    spark.range(0, event_count)
    .withColumn("event_id", F.concat(F.lit("EVT-"), F.col("id").cast("string")))
    .withColumn("customer_id", (F.col("id") % 500).cast("int"))
    .withColumn(
        "event_type",
        F.element_at(
            F.array(F.lit("page_view"), F.lit("add_to_cart"), F.lit("checkout")),
            (F.col("id") % 3 + 1).cast("int"),
        ),
    )
    .withColumn(
        "region",
        F.element_at(
            F.array(F.lit("EMEA"), F.lit("AMER"), F.lit("APAC")),
            (F.col("id") % 3 + 1).cast("int"),
        ),
    )
    .withColumn("event_date", F.expr("date'2025-01-01' + cast(id % 180 as int)"))
    .drop("id")
)

(
    events.write.format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable("testing.default.ch_enterprise_events")
)

print(f"testing.default.ch_enterprise_events created with {event_count} rows.")

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Confirm the orders table landed as many small files, this is the file
# MAGIC -- layout Predictive Optimization/OPTIMIZE would evaluate.
# MAGIC DESCRIBE DETAIL testing.default.ch_enterprise_orders;

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Enabling Predictive Optimization: account, catalog, schema, and table level
# MAGIC
# MAGIC Predictive Optimization's `ENABLE` / `DISABLE` / `INHERIT` state is layered:
# MAGIC
# MAGIC - **Account level**: the account-wide default. Enabled by default for all existing
# MAGIC   Databricks accounts since May 7th, 2025. This layer is an account console setting,
# MAGIC   not something SQL can change, see **Manual setup required** below.
# MAGIC - **Catalog level**: inherits from the account default unless explicitly overridden.
# MAGIC - **Schema level**: inherits from its catalog unless explicitly overridden.
# MAGIC - **Table level**: inherits from its schema unless explicitly overridden.
# MAGIC
# MAGIC Catalog, schema, and table level can all be set with plain SQL. Setting a lower level
# MAGIC back to `INHERIT` removes any override and falls back to whatever the level above it
# MAGIC resolves to.

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Requires being a metastore admin or the catalog owner.
# MAGIC ALTER CATALOG testing INHERIT PREDICTIVE OPTIMIZATION;

# COMMAND ----------

# MAGIC %sql
# MAGIC ALTER SCHEMA testing.default INHERIT PREDICTIVE OPTIMIZATION;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- A single table can opt in explicitly, even if the schema/catalog above it
# MAGIC -- resolve to DISABLE, or opt out even if they resolve to ENABLE.
# MAGIC ALTER TABLE testing.default.ch_enterprise_orders ENABLE PREDICTIVE OPTIMIZATION;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- DESCRIBE ... EXTENDED surfaces the effective Predictive Optimization state
# MAGIC -- Databricks resolved for each level.
# MAGIC DESCRIBE CATALOG EXTENDED testing;

# COMMAND ----------

# MAGIC %sql
# MAGIC DESCRIBE SCHEMA EXTENDED testing.default;

# COMMAND ----------

# MAGIC %sql
# MAGIC DESCRIBE TABLE EXTENDED testing.default.ch_enterprise_orders;

# COMMAND ----------

# MAGIC %md
# MAGIC ## Manual setup required (account console)
# MAGIC
# MAGIC Predictive Optimization has shipped enabled by default for every existing Databricks
# MAGIC account since May 7th, 2025, so most workspaces will not need this step. If an account
# MAGIC admin previously turned it off, or you are on an account created before the default
# MAGIC changed and it was never enabled, the account-level toggle can only be flipped from the
# MAGIC account console, not from SQL, a notebook, or the workspace UI:
# MAGIC
# MAGIC 1. Sign in to the account console as an account admin.
# MAGIC 2. Go to **Settings**, then the **Feature enablement** tab.
# MAGIC 3. Find **Predictive optimization** and toggle it on for the account.
# MAGIC
# MAGIC This is a one-time, account-wide default. Once it is on, catalog/schema/table level
# MAGIC `ENABLE` / `DISABLE` / `INHERIT` (Section 1 above) work entirely from SQL, and every
# MAGIC other section in this notebook, creating tables, `CLUSTER BY AUTO`, reading system
# MAGIC tables, running the manual equivalents, is standard SQL/PySpark with no console step.

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. How Predictive Optimization decides when to run OPTIMIZE and VACUUM
# MAGIC
# MAGIC Predictive Optimization continuously monitors managed tables and collects statistics
# MAGIC as data is written. For `OPTIMIZE`, it estimates a return on investment from:
# MAGIC
# MAGIC - **Table usage**: how often the table is actually queried.
# MAGIC - **Size of files scanned**: how much data queries against it are reading.
# MAGIC - **Estimated query speedup**: how much compacting small files would reduce that scan.
# MAGIC - **Estimated optimization cost**: the compute cost of running `OPTIMIZE` itself.
# MAGIC
# MAGIC It only schedules the operation when the estimated benefit outweighs the estimated
# MAGIC cost, aiming for a consistently positive ROI rather than optimizing on a fixed schedule.
# MAGIC Most of the benefit comes from reducing the size of files a query has to scan (data
# MAGIC skipping / pruning), not from compaction for its own sake.
# MAGIC
# MAGIC The manual equivalent, `OPTIMIZE`, is unaffected by any of this and can always be run
# MAGIC directly:

# COMMAND ----------

# MAGIC %sql
# MAGIC OPTIMIZE testing.default.ch_enterprise_orders;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Fewer, larger files after OPTIMIZE compacted the ~40 small files from Setup.
# MAGIC DESCRIBE DETAIL testing.default.ch_enterprise_orders;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- OPTIMIZE (manual or Predictive-Optimization-triggered) shows up as a normal
# MAGIC -- entry in the table's Delta history, tagged with the operation and its metrics.
# MAGIC DESCRIBE HISTORY testing.default.ch_enterprise_orders;

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. CLUSTER BY AUTO (Automatic Liquid Clustering)
# MAGIC
# MAGIC Liquid Clustering is Databricks' modern replacement for partitioning and `ZORDER`, and
# MAGIC is recommended for all table types, including streaming tables and materialized views.
# MAGIC It clusters rows around a set of columns into groups of roughly equal size, avoiding
# MAGIC the skew that partitioning can create, which improves data skipping and scan
# MAGIC efficiency. Picking the right clustering columns by hand is not always obvious.
# MAGIC
# MAGIC `CLUSTER BY AUTO` hands that decision to Predictive Optimization. It analyzes query
# MAGIC metrics against the table over time, files read/pruned, bytes read/pruned, number of
# MAGIC scans, and which columns show up in predicates/filter expressions, to propose candidate
# MAGIC clustering keys. Candidates are "shadow replayed" against historical queries (evaluated,
# MAGIC not actually re-executed) to estimate how much pruning each candidate would have
# MAGIC achieved, so the cost-benefit analysis itself costs no extra compute. The candidate with
# MAGIC the greatest pruning benefit is applied, and because it is re-evaluated as data and
# MAGIC query patterns evolve, the clustering columns can change automatically over time, again
# MAGIC only when a positive ROI is expected.
# MAGIC
# MAGIC It can be set at table creation, or added to an existing table:

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE IF NOT EXISTS testing.default.ch_enterprise_events_clustered (
# MAGIC   event_id STRING,
# MAGIC   customer_id INT,
# MAGIC   event_type STRING,
# MAGIC   region STRING,
# MAGIC   event_date DATE
# MAGIC )
# MAGIC USING DELTA
# MAGIC CLUSTER BY AUTO;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- ch_enterprise_events already exists from Setup without CLUSTER BY, so add it here.
# MAGIC ALTER TABLE testing.default.ch_enterprise_events CLUSTER BY AUTO;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- clusteringColumns reflects the columns Predictive Optimization has chosen so far.
# MAGIC -- It can be empty right after enabling CLUSTER BY AUTO on a fresh notebook run, since
# MAGIC -- the column choice itself happens asynchronously once real query history builds up.
# MAGIC DESCRIBE DETAIL testing.default.ch_enterprise_events;

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Automatic statistics collection
# MAGIC
# MAGIC Delta tables use two kinds of statistics:
# MAGIC
# MAGIC - **Data skipping statistics**: file-level (row count, column min/max), used to skip
# MAGIC   whole files that cannot match a query's predicates.
# MAGIC - **Optimizer statistics**: table-level (row count, table size, column statistics), used
# MAGIC   by the query optimizer for planning decisions like broadcast vs. shuffle join and
# MAGIC   join order.
# MAGIC
# MAGIC Normally, optimizer statistics need an explicit `ANALYZE TABLE`. With Predictive
# MAGIC Optimization enabled, statistics are instead collected automatically during writes, or
# MAGIC recomputed automatically once they go stale, no manual `ANALYZE` and no extra setup
# MAGIC required. The manual command below still works exactly the same and is useful to run on
# MAGIC demand, for example right after a large backfill:

# COMMAND ----------

# MAGIC %sql
# MAGIC ANALYZE TABLE testing.default.ch_enterprise_orders COMPUTE STATISTICS FOR ALL COLUMNS;

# COMMAND ----------

# MAGIC %sql
# MAGIC DESCRIBE TABLE EXTENDED testing.default.ch_enterprise_orders;

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5. Automatic VACUUM
# MAGIC
# MAGIC `VACUUM` deletes data files that are no longer referenced by the table's current state
# MAGIC and are older than the retention threshold, freeing storage. It is traditionally run
# MAGIC manually, often paired with `OPTIMIZE`. Predictive Optimization automates it the same
# MAGIC ROI-aware way, weighing:
# MAGIC
# MAGIC - **VACUUMable data**: how much data is actually eligible for cleanup right now.
# MAGIC - **VACUUM cost**: the estimated compute cost of running it.
# MAGIC
# MAGIC It only runs `VACUUM` when doing so is expected to be worth the compute spent, rather
# MAGIC than on a fixed interval. The retention settings it (and manual `VACUUM`) respect are
# MAGIC ordinary Delta table properties:

# COMMAND ----------

# MAGIC %sql
# MAGIC SHOW TBLPROPERTIES testing.default.ch_enterprise_orders;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Manual VACUUM, shown for comparison with what Predictive Optimization automates.
# MAGIC -- The default 7-day (168 hour) retention threshold is respected either way.
# MAGIC VACUUM testing.default.ch_enterprise_orders RETAIN 168 HOURS;

# COMMAND ----------

# MAGIC %md
# MAGIC ## 6. Monitoring Predictive Optimization through system tables
# MAGIC
# MAGIC Every operation Predictive Optimization actually runs (or decides to skip) in the
# MAGIC background is recorded in
# MAGIC `system.storage.predictive_optimization_operations_history`, one row per operation,
# MAGIC including the catalog/schema/table it targeted, which operation type it was
# MAGIC (`OPTIMIZE`, `VACUUM`, `ANALYZE`, or `CLUSTER`), and its timing. `SELECT *` is used below
# MAGIC deliberately, since the exact column set is versioned by Databricks and best inspected
# MAGIC directly rather than hard-coded here.
# MAGIC
# MAGIC The `system` catalog and its `storage` schema need to be enabled for the account (most
# MAGIC accounts already have core system schemas enabled; if `storage` specifically returns no
# MAGIC rows or access errors, an account admin can enable it from Catalog Explorer's System
# MAGIC Tables page). Rows only appear once Predictive Optimization has actually acted on a
# MAGIC table in your account, so a table created moments ago in this notebook is unlikely to
# MAGIC show up yet.

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT *
# MAGIC FROM system.storage.predictive_optimization_operations_history
# MAGIC WHERE catalog_name = 'testing'
# MAGIC   AND schema_name = 'default'
# MAGIC ORDER BY start_time DESC
# MAGIC LIMIT 20;

# COMMAND ----------

# MAGIC %md
# MAGIC ## 7. Cost and DBU considerations
# MAGIC
# MAGIC Predictive Optimization's background `OPTIMIZE` / `VACUUM` / `ANALYZE` / clustering runs
# MAGIC consume compute like any other job, billed as DBUs on serverless compute dedicated to
# MAGIC Predictive Optimization. `system.billing.usage` records that spend like any other
# MAGIC billing record, tagged with `billing_origin_product = 'PREDICTIVE_OPTIMIZATION'`, so the
# MAGIC same billing table used for regular job/warehouse cost tracking also covers it:

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC   usage_date,
# MAGIC   sku_name,
# MAGIC   usage_unit,
# MAGIC   SUM(usage_quantity) AS total_usage_quantity
# MAGIC FROM system.billing.usage
# MAGIC WHERE billing_origin_product = 'PREDICTIVE_OPTIMIZATION'
# MAGIC GROUP BY usage_date, sku_name, usage_unit
# MAGIC ORDER BY usage_date DESC
# MAGIC LIMIT 20;

# COMMAND ----------

# MAGIC %md
# MAGIC Because every scheduling decision is explicitly ROI-aware (Sections 2, 3, and 5), the
# MAGIC DBU spend from Predictive Optimization should, by design, be smaller than the query
# MAGIC savings it produces; it is meant to pay for itself rather than being a flat tax on every
# MAGIC table. Joining the query above with `system.billing.list_prices` (not shown here, since
# MAGIC prices vary by account/region/contract) converts `total_usage_quantity` into an actual
# MAGIC currency figure for cost reporting.

# COMMAND ----------

# MAGIC %md
# MAGIC ## 8. Limitations and when not to rely on it
# MAGIC
# MAGIC Predictive Optimization is on by default, so the more useful question is when you might
# MAGIC deliberately turn it off or override it for a specific table (opinions, not an official
# MAGIC Databricks recommendation):
# MAGIC
# MAGIC 1. **ROI is not your top priority.** The ROI model optimizes for cost efficiency. If
# MAGIC    read performance is mission-critical regardless of compute cost, for example sharing
# MAGIC    data with external clients, powering a latency-sensitive application, or meeting a
# MAGIC    strict performance SLA, a more aggressive, manually scheduled `OPTIMIZE` may serve the
# MAGIC    table better than an ROI-gated one.
# MAGIC 2. **You know the query pattern better than the optimizer does.** `CLUSTER BY AUTO`
# MAGIC    picks columns based on the table's overall query pattern. If a table serves multiple
# MAGIC    use cases or user groups with materially different query patterns, or if some query
# MAGIC    paths matter far more than others (a customer-facing dashboard vs. an internal ad hoc
# MAGIC    report), manually choosing clustering keys based on that business context can
# MAGIC    outperform an average-case automatic choice.
# MAGIC
# MAGIC For either case, `ALTER TABLE ... DISABLE PREDICTIVE OPTIMIZATION` (Section 1) opts a
# MAGIC single table out while leaving the account/catalog/schema default untouched for
# MAGIC everything else, and a manually chosen `CLUSTER BY (col1, col2, ...)` can replace
# MAGIC `CLUSTER BY AUTO` on that same table at any time.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Cleanup
# MAGIC
# MAGIC Drops every table this notebook created and resets the Predictive Optimization
# MAGIC overrides made along the way back to `INHERIT`.

# COMMAND ----------

# MAGIC %sql
# MAGIC ALTER TABLE testing.default.ch_enterprise_orders INHERIT PREDICTIVE OPTIMIZATION;

# COMMAND ----------

# MAGIC %sql
# MAGIC DROP TABLE IF EXISTS testing.default.ch_enterprise_orders;
# MAGIC DROP TABLE IF EXISTS testing.default.ch_enterprise_events;
# MAGIC DROP TABLE IF EXISTS testing.default.ch_enterprise_events_clustered;

# COMMAND ----------

print("CH Enterprise sample tables dropped and Predictive Optimization overrides reset.")

# COMMAND ----------

# MAGIC %md
# MAGIC **Notes:** Databricks does not publish the exact algorithm behind Predictive
# MAGIC Optimization's ROI estimates or CLUSTER BY AUTO's column selection; Sections 2, 3, 5,
# MAGIC and 8 summarize the author's interpretation of a DAIS 2025 session by Cindy Jiang and
# MAGIC Naga Bhanoori plus Databricks documentation, not Databricks' own internal
# MAGIC documentation of the algorithm. Turning Predictive Optimization on at the account level,
# MAGIC if it is off, can only be done from the account console, not from this notebook; see
# MAGIC "Manual setup required" above. Rows in
# MAGIC `system.storage.predictive_optimization_operations_history` (Section 6) and
# MAGIC `system.billing.usage` (Section 7) only appear once Predictive Optimization has actually
# MAGIC acted on tables in your account, so both queries can legitimately return zero rows in a
# MAGIC fresh or low-traffic workspace; that is expected, not a bug in the query.
