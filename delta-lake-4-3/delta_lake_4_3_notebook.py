# Databricks notebook source
# MAGIC %md
# MAGIC # Delta Lake 4.3: Selective Data Replacement with replaceUsing and replaceOn
# MAGIC
# MAGIC **Article:** [Delta Lake 4.3: Selective Data Replacement and the Unity Catalog Delta APIs](https://medium.com/@cralle/delta-lake-4-3-selective-data-replacement-5a8fba735f02?sk=30106a30098531f123bb97c91b6e5865)
# MAGIC
# MAGIC **Author:** Christian Hansen ([https://medium.com/@cralle](https://medium.com/@cralle))
# MAGIC
# MAGIC Companion notebook for the Medium article on Delta Lake 4.3. It walks through the two new DataFrame options for selectively replacing table data:
# MAGIC
# MAGIC - `replaceUsing`: replace rows where the specified columns compare equal
# MAGIC - `replaceOn`: replace rows that satisfy a user-defined condition, including NULL-safe matching
# MAGIC
# MAGIC The examples use a sample company, **CH Enterprise**, and its daily order snapshots.
# MAGIC
# MAGIC **Requirements**
# MAGIC
# MAGIC - Databricks Runtime 18.2 or later for the Python and Scala `replaceOn` / `replaceUsing` options
# MAGIC   (the SQL forms `REPLACE USING` and `REPLACE ON` are available from Databricks Runtime 16.3 and 17.1)
# MAGIC - Unity Catalog access to a catalog and schema you can write to

# COMMAND ----------

import pyspark.sql.functions as F

CATALOG = "testing"
SCHEMA = "default"
TARGET = f"{CATALOG}.{SCHEMA}.orders"
UPDATES = f"{CATALOG}.{SCHEMA}.orders_updates"

spark.sql(f"USE CATALOG {CATALOG}")
spark.sql(f"USE SCHEMA {SCHEMA}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Set up the CH Enterprise order tables
# MAGIC
# MAGIC `orders` is the target table. `orders_updates` holds a corrected batch for a single day and region,
# MAGIC plus one order for a region the target has never seen.

# COMMAND ----------

spark.sql(f"""
CREATE OR REPLACE TABLE {TARGET} (
  order_date DATE,
  region     STRING,
  order_id   STRING,
  amount     DECIMAL(12, 2)
) USING DELTA
""")

spark.sql(f"""
INSERT INTO {TARGET} VALUES
  (DATE '2026-06-01', 'EU', 'A-1', 100.00),
  (DATE '2026-06-01', 'US', 'A-2', 200.00),
  (DATE '2026-06-02', 'EU', 'A-3', 300.00)
""")

spark.sql(f"""
CREATE OR REPLACE TABLE {UPDATES} (
  order_date DATE,
  region     STRING,
  order_id   STRING,
  amount     DECIMAL(12, 2)
) USING DELTA
""")

spark.sql(f"""
INSERT INTO {UPDATES} VALUES
  (DATE '2026-06-01', 'EU',   'A-1', 150.00),
  (DATE '2026-06-01', 'EU',   'A-4',  50.00),
  (DATE '2026-06-03', 'APAC', 'A-5', 400.00)
""")

display(spark.table(TARGET).orderBy(F.col("order_date"), F.col("region"), F.col("order_id")))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. replaceUsing: replace rows whose key columns match
# MAGIC
# MAGIC Naming `order_date, region` as the match columns tells Delta to replace every target row whose
# MAGIC `(order_date, region)` pair appears in the source. No predicate string to keep in sync with the data.
# MAGIC
# MAGIC Expected result:
# MAGIC
# MAGIC - `(2026-06-01, EU)` is replaced, so `A-1` becomes 150.00 and `A-4` is added
# MAGIC - `(2026-06-01, US)` and `(2026-06-02, EU)` are untouched
# MAGIC - `(2026-06-03, APAC)` is a new pair, so `A-5` is inserted

# COMMAND ----------

updates = spark.read.table(UPDATES)

(updates.write
    .mode("overwrite")
    .option("replaceUsing", "order_date, region")
    .saveAsTable(TARGET)
)

display(spark.table(TARGET).orderBy(F.col("order_date"), F.col("region"), F.col("order_id")))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. The same operation in SQL
# MAGIC
# MAGIC `REPLACE USING` is the SQL equivalent, available on Databricks Runtime 16.3 and above.
# MAGIC This cell re-runs the replacement, which is idempotent, so the table looks the same afterwards.

# COMMAND ----------

# MAGIC %sql
# MAGIC INSERT INTO testing.default.orders
# MAGIC REPLACE USING (order_date, region)
# MAGIC SELECT * FROM testing.default.orders_updates

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM testing.default.orders ORDER BY order_date, region, order_id

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Why equality is not always enough
# MAGIC
# MAGIC Like `JOIN USING`, `REPLACE USING` matches on regular equality, where NULL is not equal to anything.
# MAGIC A target row with a NULL in a match column therefore never matches, survives the write, and leaves
# MAGIC you with a duplicate.
# MAGIC
# MAGIC The tables below reset with a NULL region on both sides to show the problem.

# COMMAND ----------

spark.sql(f"""
CREATE OR REPLACE TABLE {TARGET} (
  order_date DATE,
  region     STRING,
  order_id   STRING,
  amount     DECIMAL(12, 2)
) USING DELTA
""")

spark.sql(f"""
INSERT INTO {TARGET} VALUES
  (DATE '2026-06-01', NULL, 'B-1', 100.00),
  (DATE '2026-06-01', 'EU', 'B-2', 200.00)
""")

spark.sql(f"""
CREATE OR REPLACE TABLE {UPDATES} (
  order_date DATE,
  region     STRING,
  order_id   STRING,
  amount     DECIMAL(12, 2)
) USING DELTA
""")

spark.sql(f"""
INSERT INTO {UPDATES} VALUES
  (DATE '2026-06-01', NULL, 'B-1', 175.00)
""")

updates = spark.read.table(UPDATES)

(updates.write
    .mode("overwrite")
    .option("replaceUsing", "order_date, region")
    .saveAsTable(TARGET)
)

# B-1 appears twice: the old row at 100.00 and the new row at 175.00
display(spark.table(TARGET).orderBy(F.col("order_id"), F.col("amount")))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5. replaceOn: NULL-safe matching with a condition
# MAGIC
# MAGIC `replaceOn` takes a boolean condition instead of a column list. Alias the source with `.alias()`
# MAGIC and the target with the `targetAlias` option, then use the NULL-safe equality operator `<=>`
# MAGIC so two NULLs compare as equal.

# COMMAND ----------

spark.sql(f"""
CREATE OR REPLACE TABLE {TARGET} (
  order_date DATE,
  region     STRING,
  order_id   STRING,
  amount     DECIMAL(12, 2)
) USING DELTA
""")

spark.sql(f"""
INSERT INTO {TARGET} VALUES
  (DATE '2026-06-01', NULL, 'B-1', 100.00),
  (DATE '2026-06-01', 'EU', 'B-2', 200.00)
""")

updates = spark.read.table(UPDATES)

(updates.alias("s")
    .write
    .mode("overwrite")
    .option("targetAlias", "t")
    .option("replaceOn", "s.order_date <=> t.order_date AND s.region <=> t.region")
    .saveAsTable(TARGET)
)

# B-1 now appears once, at 175.00, and B-2 is untouched
display(spark.table(TARGET).orderBy(F.col("order_id")))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 6. Empty source: nothing is deleted
# MAGIC
# MAGIC This is the behavior that makes reruns after an upstream outage safer. With `replaceUsing` and
# MAGIC `replaceOn`, an empty source query deletes no rows. `replaceWhere` can delete the whole matching
# MAGIC range in the same situation.

# COMMAND ----------

empty_updates = spark.read.table(UPDATES).filter(F.lit(False))
print(f"Source row count: {empty_updates.count()}")

(empty_updates.write
    .mode("overwrite")
    .option("replaceUsing", "order_date, region")
    .saveAsTable(TARGET)
)

# Both rows are still here
display(spark.table(TARGET).orderBy(F.col("order_id")))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 7. Constraints worth knowing
# MAGIC
# MAGIC - In Python and Scala, `replaceOn` and `replaceUsing` cannot be combined with `replaceWhere`,
# MAGIC   `partitionOverwriteMode`, or `overwriteSchema`
# MAGIC - `replaceWhere`, `replaceOn`, and `replaceUsing` all reject subquery predicates as of Delta Lake 4.3
# MAGIC - Match columns do not have to be partition columns: partitioned, unpartitioned, and liquid
# MAGIC   clustered tables are all supported from Databricks Runtime 17.2 onwards
# MAGIC - Use `replaceUsing` by default and reach for `replaceOn` only when equality cannot express the match
# MAGIC
# MAGIC The cell below shows the combination being rejected.

# COMMAND ----------

try:
    (spark.read.table(UPDATES).write
        .mode("overwrite")
        .option("replaceUsing", "order_date, region")
        .option("replaceWhere", "region = 'EU'")
        .saveAsTable(TARGET)
    )
except Exception as e:
    print(f"Rejected as expected: {e}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 8. Clean up

# COMMAND ----------

spark.sql(f"DROP TABLE IF EXISTS {TARGET}")
spark.sql(f"DROP TABLE IF EXISTS {UPDATES}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Notes and sources
# MAGIC
# MAGIC Delta Lake 4.3.0 was released in June 2026 and is built on Apache Spark 4.1.0 and 4.0.1. On Databricks,
# MAGIC the SQL forms of these operations landed earlier than the DataFrame options, so check your runtime
# MAGIC version before refactoring a pipeline.
# MAGIC
# MAGIC Sources:
# MAGIC
# MAGIC - [Delta Lake 4.3.0 release notes, GitHub](https://github.com/delta-io/delta/releases/tag/v4.3.0)
# MAGIC - [Selectively overwrite data with Delta Lake, Databricks docs](https://docs.databricks.com/aws/en/delta/selective-overwrite)
# MAGIC - [Strengthening Catalog-Managed Delta Tables with the Unity Catalog Delta APIs, Delta Lake blog](https://delta.io/blog/2026-06-22-delta-4-3-release/)
