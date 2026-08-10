# Databricks notebook source
# MAGIC %md
# MAGIC # Auto-TTL in Databricks: Automatic Row Deletion That Actually Works
# MAGIC
# MAGIC **Article:** [Auto-TTL in Databricks: Automatic Row Deletion That Actually Works](https://medium.com/@cralle/auto-ttl-in-databricks-automated-data-retention-done-properly-5ea511b45c1d?sk=105895af20d93c0c0cd4b507029229e2)
# MAGIC
# MAGIC **Author:** Christian Hansen (https://medium.com/@cralle)
# MAGIC
# MAGIC This notebook covers setting an Auto-TTL policy at creation time and on an existing table, verifying and removing a policy, reading from an Auto-TTL table with Structured Streaming, and monitoring Auto-TTL activity and cost through system tables.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Setup: Create example tables

# COMMAND ----------

# MAGIC %sql
# MAGIC USE CATALOG testing;
# MAGIC USE SCHEMA default;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Create a fresh table for testing Auto-TTL
# MAGIC DROP TABLE IF EXISTS testing.default.user_events;
# MAGIC
# MAGIC CREATE TABLE testing.default.user_events (
# MAGIC   user_id     BIGINT,
# MAGIC   event_type  STRING,
# MAGIC   event_time  TIMESTAMP,
# MAGIC   payload     STRING
# MAGIC );

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Insert some sample data: mix of recent and old events
# MAGIC INSERT INTO testing.default.user_events VALUES
# MAGIC   (1, 'click',    current_timestamp() - INTERVAL 5  DAY, 'recent event'),
# MAGIC   (2, 'view',     current_timestamp() - INTERVAL 15 DAY, 'recent event'),
# MAGIC   (3, 'purchase', current_timestamp() - INTERVAL 45 DAY, 'old event - will expire'),
# MAGIC   (4, 'click',    current_timestamp() - INTERVAL 60 DAY, 'old event - will expire'),
# MAGIC   (5, 'logout',   current_timestamp() - INTERVAL 95 DAY, 'very old event - will expire');

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM testing.default.user_events ORDER BY event_time;

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Set an Auto-TTL policy on an existing table

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Retain rows for 30 days after event_time
# MAGIC ALTER TABLE testing.default.user_events DELETE ROWS 30 DAYS AFTER event_time;

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Create a new table with Auto-TTL at definition time

# COMMAND ----------

# MAGIC %sql
# MAGIC DROP TABLE IF EXISTS testing.default.user_events_with_ttl;
# MAGIC
# MAGIC CREATE TABLE testing.default.user_events_with_ttl (
# MAGIC   user_id     BIGINT,
# MAGIC   event_type  STRING,
# MAGIC   event_time  TIMESTAMP,
# MAGIC   payload     STRING
# MAGIC )
# MAGIC DELETE ROWS 90 DAYS AFTER event_time;

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Verify the Auto-TTL policy is set

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Full table description; look for autottl.expireInDays and autottl.timestampColumn
# MAGIC DESCRIBE TABLE EXTENDED testing.default.user_events;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Just the table properties
# MAGIC SHOW TBLPROPERTIES testing.default.user_events;

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Structured Streaming read from a table with Auto-TTL
# MAGIC
# MAGIC When reading from a table with Auto-TTL using Structured Streaming, always set
# MAGIC `skipChangeCommits = true`. Auto-TTL deletes appear as data changes in the Delta
# MAGIC log. Without this option, the stream fails when Auto-TTL runs.

# COMMAND ----------

# Read from a table that has Auto-TTL enabled
df = (
    spark.readStream
    .format("delta")
    .option("skipChangeCommits", "true")
    .table("testing.default.user_events")
)

# This notebook drops and recreates user_events on every run, so the table gets a
# new Delta identity each time. A reused checkpoint would still point at the old
# table and fail, so start the stream from a clean checkpoint and a clean target.
checkpoint_path = "/tmp/auto_ttl_demo_checkpoint"
dbutils.fs.rm(checkpoint_path, True)
spark.sql("DROP TABLE IF EXISTS testing.default.user_events_downstream")

# Example: write to a downstream table
# Use trigger(availableNow=True) on serverless clusters (ProcessingTime is not supported)
query = (
    df.writeStream
    .format("delta")
    .outputMode("append")
    .trigger(availableNow=True)
    .option("checkpointLocation", checkpoint_path)
    .toTable("testing.default.user_events_downstream")
)

# Wait for the trigger to complete
query.awaitTermination()

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5. Monitor Auto-TTL operations via system tables

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Review DELETE, PURGE, and VACUUM operations from Auto-TTL in the past 7 days
# MAGIC WITH tables_with_deletes AS (
# MAGIC   SELECT DISTINCT catalog_name, schema_name, table_name
# MAGIC   FROM system.storage.predictive_optimization_operations_history
# MAGIC   WHERE
# MAGIC     operation_type = 'DELETE'
# MAGIC     AND timestampdiff(day, start_time, now()) < 7
# MAGIC )
# MAGIC SELECT hist.*
# MAGIC FROM system.storage.predictive_optimization_operations_history AS hist
# MAGIC INNER JOIN tables_with_deletes AS t
# MAGIC   ON hist.catalog_name = t.catalog_name
# MAGIC   AND hist.schema_name = t.schema_name
# MAGIC   AND hist.table_name = t.table_name
# MAGIC WHERE
# MAGIC   hist.operation_type IN ('DELETE', 'PURGE', 'VACUUM')
# MAGIC   AND timestampdiff(day, hist.start_time, now()) < 7
# MAGIC ORDER BY hist.start_time DESC;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Estimate DBU cost of Auto-TTL operations in the past 30 days
# MAGIC WITH tables_with_deletes AS (
# MAGIC   SELECT DISTINCT table_name
# MAGIC   FROM system.storage.predictive_optimization_operations_history
# MAGIC   WHERE
# MAGIC     operation_type = 'DELETE'
# MAGIC     AND timestampdiff(day, start_time, now()) < 30
# MAGIC )
# MAGIC SELECT SUM(usage_quantity) AS total_estimated_dbu
# MAGIC FROM system.storage.predictive_optimization_operations_history AS hist
# MAGIC INNER JOIN tables_with_deletes AS t
# MAGIC   ON hist.table_name = t.table_name
# MAGIC WHERE
# MAGIC   hist.operation_type IN ('DELETE', 'PURGE', 'VACUUM')
# MAGIC   AND hist.usage_unit = 'ESTIMATED_DBU'
# MAGIC   AND timestampdiff(day, hist.start_time, now()) < 30;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Review operation history on a specific table
# MAGIC DESCRIBE HISTORY testing.default.user_events;

# COMMAND ----------

# MAGIC %md
# MAGIC ## 6. Remove an Auto-TTL policy

# COMMAND ----------

# MAGIC %sql
# MAGIC ALTER TABLE testing.default.user_events DROP ROW DELETION;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Confirm the policy has been removed
# MAGIC SHOW TBLPROPERTIES testing.default.user_events;

# COMMAND ----------

# MAGIC %md
# MAGIC ## Cleanup

# COMMAND ----------

# MAGIC %sql
# MAGIC DROP TABLE IF EXISTS testing.default.user_events;
# MAGIC DROP TABLE IF EXISTS testing.default.user_events_with_ttl;
# MAGIC DROP TABLE IF EXISTS testing.default.user_events_downstream;

# COMMAND ----------

# Remove the streaming checkpoint so the notebook can be re-run cleanly
dbutils.fs.rm("/tmp/auto_ttl_demo_checkpoint", True)

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC **Source:** [Automatic row deletion with auto time-to-live](https://learn.microsoft.com/en-us/azure/databricks/tables/operations/auto-ttl)
# MAGIC
# MAGIC **Notes:**
# MAGIC - Auto-TTL requires Predictive Optimization to be enabled
# MAGIC - Requires Databricks Runtime 17.3 or above to set policies
# MAGIC - DBR 17.2 and below can read/write tables with Auto-TTL but cannot manage policies
# MAGIC - Deletion is asynchronous; use system tables to verify deletions occurred
# MAGIC - Buffer time between row expiration and permanent deletion can be up to 6 days plus the data retention period (default 7 days)
# MAGIC - Always set `skipChangeCommits = true` on streaming reads from Auto-TTL tables
