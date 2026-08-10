# Databricks notebook source
# MAGIC %md
# MAGIC # How Delta Lake Time Travel and VACUUM Retention Now Work in Databricks
# MAGIC
# MAGIC **Article:** [How Delta Lake Time Travel and VACUUM Retention Now Work in Databricks](https://medium.com/@cralle/important-changes-coming-to-delta-lake-time-travel-databricks-december-2025-644b6fd03d9e?sk=2a5512a5842cf798fe00d4a884d55997)
# MAGIC
# MAGIC By Christian Hansen. Published November 3, 2025.
# MAGIC
# MAGIC Hands-on walkthrough of the December 2025 changes to Delta Lake time travel and
# MAGIC `VACUUM` retention, using a CH Enterprise sample table with several versioned writes
# MAGIC so the behavior can actually be demonstrated.

# COMMAND ----------

# MAGIC %md
# MAGIC Starting December 2025, Databricks changes how Delta Lake time travel and `VACUUM`
# MAGIC interact:
# MAGIC
# MAGIC 1. Time travel (`VERSION AS OF`, `TIMESTAMP AS OF`, and anything else using `AS OF`,
# MAGIC    including `RESTORE`, CDC, and `CLONE`) is strictly bounded by the table property
# MAGIC    `delta.deletedFileRetentionDuration`. Query beyond that window and you get an error,
# MAGIC    instead of an answer that depends on when `VACUUM` last ran.
# MAGIC 2. The `RETAIN n HOURS` argument on `VACUUM` is ignored, except for the special case
# MAGIC    `RETAIN 0 HOURS`, which is still honored (it deletes all history immediately).
# MAGIC    Retention is otherwise fully governed by `delta.deletedFileRetentionDuration`.
# MAGIC 3. `delta.logRetentionDuration` must be greater than or equal to
# MAGIC    `delta.deletedFileRetentionDuration`.
# MAGIC
# MAGIC This notebook builds a small CH Enterprise sample table, writes several versions to
# MAGIC it, and then walks through each concept with a runnable demo: time travel by version
# MAGIC number and by timestamp, `DESCRIBE HISTORY`, the table properties that control
# MAGIC retention, and how `VACUUM` now interacts with time travel.
# MAGIC
# MAGIC **Honest limitation:** the real behavior described in the article is bounded by
# MAGIC *elapsed wall-clock time* (retention durations measured in days). A notebook run
# MAGIC cannot fast-forward the clock, so this notebook cannot prove that a query 8 days back
# MAGIC fails against a 7-day retention setting. Where that's the case, the markdown below says
# MAGIC so directly instead of faking a result. The one edge case that shows the interaction
# MAGIC synchronously is `VACUUM ... RETAIN 0 HOURS`, which deletes history immediately; its
# MAGIC code is included below as a reference-only block, since it needs a Spark safety
# MAGIC configuration that serverless compute does not allow and so must be run on classic compute.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Setup
# MAGIC
# MAGIC Create a CH Enterprise sample table (`testing.default.ch_enterprise_orders`) and
# MAGIC write several versions to it: an initial load, a status update, a delete, a merge, and
# MAGIC another update. This gives us a real, multi-version Delta table to time travel across
# MAGIC for the rest of the notebook.

# COMMAND ----------

import pyspark.sql.functions as F

catalog = "testing"
schema = "default"
table_name = f"{catalog}.{schema}.ch_enterprise_orders"

spark.sql(f"CREATE CATALOG IF NOT EXISTS {catalog}")
spark.sql(f"CREATE SCHEMA IF NOT EXISTS {catalog}.{schema}")
spark.sql(f"DROP TABLE IF EXISTS {table_name}")

spark.sql(f"""
    CREATE TABLE {table_name} (
        order_id INT,
        customer STRING,
        amount DOUBLE,
        status STRING
    ) USING DELTA
""")

# COMMAND ----------

# Write several versions to the table so there is real history to time travel across.
versioned_writes = [
    # Version: initial load
    f"""
    INSERT INTO {table_name} VALUES
        (1, 'Acme Corp', 1200.00, 'OPEN'),
        (2, 'Globex Inc', 450.50, 'OPEN'),
        (3, 'Initech', 899.99, 'OPEN'),
        (4, 'Umbrella LLC', 2300.00, 'OPEN'),
        (5, 'Wayne Enterprises', 75.25, 'OPEN')
    """,
    # Version: mark two orders as shipped
    f"UPDATE {table_name} SET status = 'SHIPPED' WHERE order_id IN (1, 2)",
    # Version: a cancelled order gets removed
    f"DELETE FROM {table_name} WHERE order_id = 5",
    # Version: merge in a price correction and a brand new order
    f"""
    MERGE INTO {table_name} AS target
    USING (
        SELECT 3 AS order_id, 'Initech' AS customer, 950.00 AS amount, 'SHIPPED' AS status
        UNION ALL
        SELECT 6 AS order_id, 'Stark Industries' AS customer, 3100.00 AS amount, 'OPEN' AS status
    ) AS source
    ON target.order_id = source.order_id
    WHEN MATCHED THEN UPDATE SET target.amount = source.amount, target.status = source.status
    WHEN NOT MATCHED THEN INSERT (order_id, customer, amount, status)
        VALUES (source.order_id, source.customer, source.amount, source.status)
    """,
    # Version: close out everything that shipped
    f"UPDATE {table_name} SET status = 'CLOSED' WHERE status = 'SHIPPED'",
]

for statement in versioned_writes:
    spark.sql(statement)

row_count = spark.table(table_name).count()
print(f"{table_name} now has {row_count} rows spread across {len(versioned_writes) + 1} Delta versions (including the CREATE TABLE version).")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. `DESCRIBE HISTORY`: seeing every version of a table
# MAGIC
# MAGIC `DESCRIBE HISTORY` returns one row per Delta transaction: the version number, the
# MAGIC timestamp it committed, the operation, and the parameters passed to it. This is the
# MAGIC first place to look when you need to know what versions actually exist, and it's what
# MAGIC we use below to pick real version numbers and timestamps for the time travel demos.

# COMMAND ----------

history_df = spark.sql(f"DESCRIBE HISTORY {table_name}")
display(
    history_df
    .select("version", "timestamp", "operation", "operationParameters")
    .orderBy(F.col("version").asc())
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Time travel by version number
# MAGIC
# MAGIC `SELECT * FROM my_table VERSION AS OF <version>` returns the table exactly as it
# MAGIC looked right after that version committed. Below we pull the earliest and latest
# MAGIC version numbers straight out of `DESCRIBE HISTORY` and compare row counts, which
# MAGIC should differ because of the insert, delete, and merge we ran during setup.

# COMMAND ----------

history_rows = history_df.orderBy(F.col("version").asc()).collect()
first_version = history_rows[0]["version"]
latest_version = history_rows[-1]["version"]

first_version_df = spark.sql(f"SELECT * FROM {table_name} VERSION AS OF {first_version}")
latest_version_df = spark.sql(f"SELECT * FROM {table_name} VERSION AS OF {latest_version}")

print(f"Version {first_version} row count: {first_version_df.count()}")
print(f"Version {latest_version} row count: {latest_version_df.count()}")

display(first_version_df.orderBy("order_id"))
display(latest_version_df.orderBy("order_id"))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Time travel by timestamp
# MAGIC
# MAGIC `SELECT * FROM my_table TIMESTAMP AS OF '<timestamp>'` works the same way, but keyed
# MAGIC off a timestamp instead of a version number. We grab the commit timestamp of the
# MAGIC version right after the initial load from `DESCRIBE HISTORY` and use it directly, so
# MAGIC the timestamp used here is a real commit time from this table's own history rather
# MAGIC than a made-up value.

# COMMAND ----------

# The second row (index 1) is the version right after the initial INSERT.
target_row = history_rows[1]
target_version = target_row["version"]
target_timestamp = str(target_row["timestamp"])

print(f"Time traveling to the timestamp of version {target_version}: {target_timestamp}")

timestamp_df = spark.sql(f"SELECT * FROM {table_name} TIMESTAMP AS OF '{target_timestamp}'")
display(timestamp_df.orderBy("order_id"))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Table properties that control retention
# MAGIC
# MAGIC Two table properties govern how much history a Delta table keeps around:
# MAGIC
# MAGIC - `delta.deletedFileRetentionDuration`: how long deleted/overwritten data files are
# MAGIC   kept before `VACUUM` can remove them. Default is 7 days. From December 2025, this
# MAGIC   is also the hard limit on how far back `AS OF` queries can go.
# MAGIC - `delta.logRetentionDuration`: how long Delta transaction log entries are kept.
# MAGIC   Default is 30 days. It must be set greater than or equal to
# MAGIC   `delta.deletedFileRetentionDuration`, otherwise the log could expire before the data
# MAGIC   files it describes do.
# MAGIC
# MAGIC The example below raises both to 30 days, which is a realistic setting for a table
# MAGIC where analysts occasionally need to time travel back a few weeks.

# COMMAND ----------

spark.sql(f"""
    ALTER TABLE {table_name} SET TBLPROPERTIES (
        'delta.deletedFileRetentionDuration' = 'interval 30 days',
        'delta.logRetentionDuration' = 'interval 30 days'
    )
""")

display(
    spark.sql(f"SHOW TBLPROPERTIES {table_name}")
    .where(F.col("key").isin("delta.deletedFileRetentionDuration", "delta.logRetentionDuration"))
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5. How retention settings bound how far back you can travel
# MAGIC
# MAGIC From December 2025, `delta.deletedFileRetentionDuration` is the hard ceiling on time
# MAGIC travel: query further back than that window and the `AS OF` query fails, regardless of
# MAGIC when `VACUUM` last ran. Previously the real answer depended on `VACUUM` history, which
# MAGIC is exactly the inconsistency this change removes.
# MAGIC
# MAGIC **This part cannot be fully demonstrated synchronously.** Proving that a 30-day
# MAGIC retention window blocks a 31-day-old query means the underlying data files have to
# MAGIC have actually aged out, which takes real elapsed time, not something a notebook cell
# MAGIC can fake or fast-forward. Setting `delta.deletedFileRetentionDuration` to a few
# MAGIC seconds and then sleeping past it in the same session would not be a faithful
# MAGIC reproduction of the real mechanism either, since Databricks does not guarantee VACUUM
# MAGIC or retention enforcement work at second-level granularity, so no such shortcut is used
# MAGIC here.
# MAGIC
# MAGIC The one exception in the new rules that shows retention and time travel interact for
# MAGIC real is `VACUUM ... RETAIN 0 HOURS`, which is still honored and deletes all history
# MAGIC immediately. That code is shown in the next section, as a reference-only block:
# MAGIC running it requires lowering a Spark safety configuration that serverless compute does
# MAGIC not allow, so run it on classic compute to see the failure for yourself.

# COMMAND ----------

# MAGIC %md
# MAGIC ## 6. `VACUUM` behavior and its interaction with time travel
# MAGIC
# MAGIC Under the new behavior, `VACUUM my_table RETAIN <n> HOURS` ignores the `RETAIN`
# MAGIC argument entirely and instead uses `delta.deletedFileRetentionDuration`, with one
# MAGIC exception: `RETAIN 0 HOURS` is still honored, and immediately deletes all
# MAGIC history-backing files.
# MAGIC
# MAGIC The reference-only block below uses that exception to show the interaction: after
# MAGIC `VACUUM ... RETAIN 0 HOURS`, the earliest version's data files are gone, so time
# MAGIC traveling back to it fails. Note this only shows the "delete everything now" edge case;
# MAGIC it does not, and cannot, prove the elapsed-time-based expiry described in Section 5
# MAGIC above.
# MAGIC
# MAGIC A real `VACUUM` (without `RETAIN 0 HOURS`) requires files to actually be older than the
# MAGIC retention window before it will remove them, so running plain `VACUUM {table_name}`
# MAGIC right after setup would not delete anything yet, since every file written above is only
# MAGIC seconds old.

# COMMAND ----------

# MAGIC %md
# MAGIC ### Reference only: `VACUUM ... RETAIN 0 HOURS` and time travel
# MAGIC
# MAGIC The steps below demonstrate the interaction for real, but they cannot run on
# MAGIC serverless compute: `VACUUM ... RETAIN 0 HOURS` deletes files newer than the retention
# MAGIC window, so it requires lowering the `spark.databricks.delta.retentionDurationCheck.enabled`
# MAGIC safety check first, and that Spark configuration cannot be set on serverless (it raises
# MAGIC `CONFIG_NOT_AVAILABLE`). Run this in a workspace on classic (non-serverless) compute to
# MAGIC see the failure for real.
# MAGIC
# MAGIC ```python
# MAGIC # Lower the safety check so we can use RETAIN 0 HOURS, which is the one argument
# MAGIC # VACUUM still honors under the new rules. This is a deliberately destructive demo step.
# MAGIC spark.sql("SET spark.databricks.delta.retentionDurationCheck.enabled = false")
# MAGIC
# MAGIC vacuum_result = spark.sql(f"VACUUM {table_name} RETAIN 0 HOURS")
# MAGIC display(vacuum_result)
# MAGIC
# MAGIC # Time traveling back to the earliest version should now fail: VACUUM RETAIN 0 HOURS
# MAGIC # just removed the data files that version depends on.
# MAGIC try:
# MAGIC     spark.sql(f"SELECT * FROM {table_name} VERSION AS OF {first_version}").collect()
# MAGIC     print(f"Unexpectedly succeeded reading version {first_version} after VACUUM RETAIN 0 HOURS.")
# MAGIC except Exception as e:
# MAGIC     print(f"Time travel to version {first_version} failed as expected after VACUUM RETAIN 0 HOURS:")
# MAGIC     print(e)
# MAGIC ```

# COMMAND ----------

# MAGIC %md
# MAGIC ## 7. Checking whether you already rely on time travel (reference only)
# MAGIC
# MAGIC Before this ships to a given workspace, it's worth checking whether anything is
# MAGIC already querying with `AS OF`. The article suggests scanning query history for it:
# MAGIC
# MAGIC ```sql
# MAGIC SELECT *
# MAGIC FROM system.query.history
# MAGIC WHERE LOWER(statement_text) LIKE '%as of%';
# MAGIC ```
# MAGIC
# MAGIC This is shown as reference rather than executed in this notebook: it depends on the
# MAGIC `system.query.history` table being enabled for the workspace, and on enough time
# MAGIC having passed for the relevant queries to actually show up in it, neither of which this
# MAGIC notebook can guarantee. Run it directly in your own workspace against `system.query.history`
# MAGIC to check real usage.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Cleanup
# MAGIC
# MAGIC Drop the sample table to leave the workspace as it found it.

# COMMAND ----------

spark.sql(f"DROP TABLE IF EXISTS {table_name}")
print(f"Dropped {table_name}.")
