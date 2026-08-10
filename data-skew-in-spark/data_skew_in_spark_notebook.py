# Databricks notebook source
# MAGIC %md
# MAGIC # How to Fix Data Skew in Apache Spark and Databricks: AQE, Repartitioning, and Salting
# MAGIC
# MAGIC **Author:** Christian Hansen ([https://medium.com/@cralle](https://medium.com/@cralle))
# MAGIC
# MAGIC **Published:** September 30, 2025
# MAGIC
# MAGIC A runnable walkthrough for diagnosing and fixing data skew in Apache Spark and Databricks: spotting it with partition-size and frequency checks, then comparing four fixes head to head, Adaptive Query Execution (AQE), `repartition()`, tuning `spark.sql.shuffle.partitions`, and key salting, against a genuinely skewed CH Enterprise sample dataset. Sample data lives in `testing.default`.
# MAGIC
# MAGIC **Article:** [How to Fix Data Skew in Apache Spark and Databricks: AQE, Repartitioning, and Salting](https://medium.com/@cralle/handling-data-skew-in-databricks-and-pyspark-7a16dc227a09?sk=d231b048f8d17f5efd89adb14c97a9cc)
# MAGIC
# MAGIC **Requires:** a Unity Catalog enabled workspace with `CREATE TABLE` privilege on `testing.default`. Adaptive Query Execution and its skew-join handling ship enabled by default in Databricks, so no special runtime version is needed. Opening the Spark UI's Stages tab and Shuffle Read Size column to inspect a running job is described inline rather than executed, since that view only exists once a job runs on a live cluster.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Setup
# MAGIC
# MAGIC Creates a genuinely skewed CH Enterprise sample dataset: a table of order events where one customer, `CUST-DOMINANT`, accounts for the large majority of rows while four other customers share the rest. The row counts below are scaled down from the volumes in the article (tens of millions of rows) to sizes that run comfortably in a single notebook, while keeping roughly the same skew ratio between the largest and smallest group, so the imbalance is still clearly visible in partition sizes and join timings. The table is written to `testing.default.ch_skewed_orders` and dropped again in Cleanup.

# COMMAND ----------

import pyspark.sql.functions as F
import time

catalog = "testing"
schema = "default"
table_name = f"{catalog}.{schema}.ch_skewed_orders"

spark.sql(f"USE CATALOG {catalog}")
spark.sql(f"USE SCHEMA {schema}")


def try_set_conf(key, value):
    """Set a Spark config, tolerating runtimes where it is locked.

    On a classic cluster the AQE toggles below apply exactly as written. On
    serverless compute some of them (the AQE ones) are read-only and raise
    CONFIG_NOT_AVAILABLE; there AQE is always on, so we simply note it and
    continue rather than failing the notebook.
    """
    try:
        spark.conf.set(key, value)
        print(f"Set {key} = {value}")
    except Exception as e:
        print(f"Note: {key} is locked on this compute, leaving it at its default. ({str(e).splitlines()[0]})")

# COMMAND ----------

# Row counts per customer, scaled down from the article's tens-of-millions example,
# but keeping a similar ~80x skew ratio between the largest and smallest group.
customer_row_counts = [
    ("CUST-DOMINANT", 2_000_000),
    ("CUST-0002", 250_000),
    ("CUST-0003", 250_000),
    ("CUST-0004", 100_000),
    ("CUST-0005", 25_000),
]

customer_dfs = []
start_id = 0
for customer_id, row_count in customer_row_counts:
    customer_df = (
        spark.range(start_id, start_id + row_count)
        .withColumn("customer_id", F.lit(customer_id))
        .withColumn("amount", (F.rand() * 1000).cast("int"))
        .select(F.col("id").cast("int").alias("order_id"), "customer_id", "amount")
    )
    customer_dfs.append(customer_df)
    start_id += row_count

df_skewed_orders = customer_dfs[0]
for customer_df in customer_dfs[1:]:
    df_skewed_orders = df_skewed_orders.union(customer_df)

(
    df_skewed_orders.write.format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .partitionBy("customer_id")
    .saveAsTable(table_name)
)

total_rows = sum(row_count for _, row_count in customer_row_counts)
print(f"Setup complete: {table_name} created with {total_rows} rows")

# COMMAND ----------

df_skewed_orders = spark.read.table(table_name)

df_customer_lookup = spark.createDataFrame(
    [
        ("CUST-DOMINANT", "Flagship enterprise contract"),
        ("CUST-0002", "Standard account"),
        ("CUST-0003", "Standard account"),
        ("CUST-0004", "Standard account"),
        ("CUST-0005", "Standard account"),
    ],
    ["customer_id", "customer_tier"],
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Identify Data Skew
# MAGIC
# MAGIC Before reaching for a fix, confirm the job actually has skew. The article walks through four checks:
# MAGIC
# MAGIC - **Slow-running tasks:** in the Databricks Spark UI, open the Stages tab for the job. In a healthy stage, tasks finish in roughly the same time; a stage where one or two tasks run for minutes while the rest finish in seconds points at skewed partitions.
# MAGIC - **Partition sizes:** inspect the row count per partition directly (below).
# MAGIC - **Shuffle read/write metrics:** in the Spark UI, check the Shuffle Read Size column for the stage; if a few tasks read hundreds of MBs or GBs while others read almost nothing, that stage has skew.
# MAGIC - **Data distribution:** a quick frequency count on the join or group-by key (below). Rule of thumb: if a few keys are 10 to 100x larger than the median, you are dealing with skew.
# MAGIC
# MAGIC The first two checks (Stages tab, Shuffle Read Size) only exist once a job has actually run on a live cluster, so they are described here rather than executed. The partition-size and frequency checks below are runnable and will show the skew in `ch_skewed_orders` directly.

# COMMAND ----------

partition_counts = (
    df_skewed_orders
    .withColumn("partition_id", F.spark_partition_id())
    .groupBy("partition_id")
    .count()
    .orderBy("partition_id")
)
partition_sizes = [row["count"] for row in partition_counts.collect()]

print("Partition sizes:", partition_sizes)
print(
    "Max:", max(partition_sizes),
    "Min:", min(partition_sizes),
    "Avg:", sum(partition_sizes) / len(partition_sizes),
)

# COMMAND ----------

(
    df_skewed_orders.groupBy("customer_id")
    .count()
    .orderBy(F.desc("count"))
    .show(10, truncate=False)
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Baseline: Join With No Skew Handling
# MAGIC
# MAGIC To make the comparisons below meaningful, this cell explicitly turns Adaptive Query Execution off so the baseline reflects a plain sort-merge join with no adaptive rebalancing. This is only for this demo; Databricks enables AQE by default and disabling it is not recommended in production. On serverless compute the AQE toggles are read-only (AQE is always on), so the `try_set_conf` helper reports that and the baseline runs under AQE; on a classic cluster it applies as written. The join is on `customer_id`, the skewed key, against the small `df_customer_lookup` table, and every technique below times the same join with `.count()` to force it to actually execute.

# COMMAND ----------

try_set_conf("spark.sql.adaptive.enabled", "false")

start_time = time.time()
df_baseline_join = df_skewed_orders.join(df_customer_lookup, on="customer_id", how="inner")
baseline_row_count = df_baseline_join.count()
baseline_seconds = time.time() - start_time

print(f"Baseline join row count: {baseline_row_count}")
print(f"Baseline join elapsed time: {baseline_seconds:.1f} seconds")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Adaptive Query Execution (AQE)
# MAGIC
# MAGIC AQE re-optimizes a query plan at runtime based on the statistics Spark actually observes, instead of relying only on estimates made before the query runs. It is most useful when table statistics are missing or inaccurate, the data distribution is unknown or skewed, or the query is complex. In Databricks it is enabled by default. Its skew-join handling specifically detects oversized partitions during a shuffle and splits them into multiple smaller tasks, on top of its other capabilities (switching join strategies, coalescing small partitions, optimizing empty relations). AQE narrows the imbalance but, on its own, will not fully eliminate a very hot key like `CUST-DOMINANT`.

# COMMAND ----------

try_set_conf("spark.sql.adaptive.enabled", "true")
try_set_conf("spark.sql.adaptive.skewJoin.enabled", "true")

start_time = time.time()
df_aqe_join = df_skewed_orders.join(df_customer_lookup, on="customer_id", how="inner")
aqe_row_count = df_aqe_join.count()
aqe_seconds = time.time() - start_time

print(f"AQE join row count: {aqe_row_count}")
print(f"AQE join elapsed time: {aqe_seconds:.1f} seconds")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Repartitioning
# MAGIC
# MAGIC `repartition()` on the join key forces an explicit shuffle into a chosen number of partitions. It is a quick, low-effort fix for moderate skew and spare cluster capacity, and it usually narrows the gap between the smallest and largest task. It does not remove the hot key itself: every row for `CUST-DOMINANT` still lands in the same partition, just alongside a more evenly sized set of partitions for everyone else.

# COMMAND ----------

df_repartitioned_orders = df_skewed_orders.repartition(200, "customer_id")

start_time = time.time()
df_repartition_join = df_repartitioned_orders.join(df_customer_lookup, on="customer_id", how="inner")
repartition_row_count = df_repartition_join.count()
repartition_seconds = time.time() - start_time

print(f"Repartitioned join row count: {repartition_row_count}")
print(f"Repartitioned join elapsed time: {repartition_seconds:.1f} seconds")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5. Shuffle Partitions Config
# MAGIC
# MAGIC `spark.sql.shuffle.partitions` controls how many partitions a shuffle (join, `groupBy`, `distinct`) produces; the default is 200. Raising it breaks large partitions into more, smaller pieces and can spread work more evenly across the cluster. Like plain repartitioning, it is a parallelism knob, not a fix for the underlying hot key: `CUST-DOMINANT` will still dominate whichever partition(s) it lands in. Treat it as a quick first step for moderate skew, and reset it back to the default afterwards so it does not affect later cells.

# COMMAND ----------

default_shuffle_partitions = spark.conf.get("spark.sql.shuffle.partitions")
print(f"Default shuffle partitions: {default_shuffle_partitions}")

spark.conf.set("spark.sql.shuffle.partitions", "500")

start_time = time.time()
df_shuffle_tuned_join = df_skewed_orders.join(df_customer_lookup, on="customer_id", how="inner")
shuffle_tuned_row_count = df_shuffle_tuned_join.count()
shuffle_tuned_seconds = time.time() - start_time

print(f"Shuffle-partitions-tuned join row count: {shuffle_tuned_row_count}")
print(f"Shuffle-partitions-tuned join elapsed time: {shuffle_tuned_seconds:.1f} seconds")

spark.conf.set("spark.sql.shuffle.partitions", default_shuffle_partitions)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 6. Salting
# MAGIC
# MAGIC Salting is the highest-effort fix, and the most effective one for severe skew: append a random "salt" value to the skewed key on both sides of the join, so a single hot key gets split across several synthetic sub-keys instead of landing in one partition. `df_skewed_orders` gets a random `salt` column (0 to `NUM_SALT_BUCKETS - 1`); `df_customer_lookup` is cross-joined against the same salt range so every customer has a matching row for every salt value; then the join happens on `["customer_id", "salt"]` instead of `"customer_id"` alone. The extra salt column costs a little more data, but the hot key now spreads evenly across `NUM_SALT_BUCKETS` partitions instead of one.

# COMMAND ----------

NUM_SALT_BUCKETS = 10

df_skewed_orders_salted = (
    df_skewed_orders
    .withColumn("salt", (F.rand() * NUM_SALT_BUCKETS).cast("int"))
)

df_salted_customer_lookup = (
    df_customer_lookup
    .crossJoin(spark.range(NUM_SALT_BUCKETS).withColumnRenamed("id", "salt"))
)

start_time = time.time()
df_salted_join = df_skewed_orders_salted.join(
    df_salted_customer_lookup, on=["customer_id", "salt"], how="inner"
)
salted_row_count = df_salted_join.count()
salted_seconds = time.time() - start_time

print(f"Salted join row count: {salted_row_count}")
print(f"Salted join elapsed time: {salted_seconds:.1f} seconds")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 7. Comparing the Results
# MAGIC
# MAGIC All five joins above return the same row count; only the elapsed time and the evenness of the underlying task distribution differ. The table below prints the timings captured in each section side by side. Exact numbers depend on the cluster this notebook runs on, but the relative ordering (baseline slowest, salting fastest, AQE and repartitioning in between) should hold on most clusters given how heavily `CUST-DOMINANT` dominates this dataset.

# COMMAND ----------

comparison = [
    ("Baseline (AQE off)", baseline_seconds, baseline_row_count),
    ("AQE (skew join handling)", aqe_seconds, aqe_row_count),
    ("Repartition", repartition_seconds, repartition_row_count),
    ("Shuffle partitions tuning", shuffle_tuned_seconds, shuffle_tuned_row_count),
    ("Salting", salted_seconds, salted_row_count),
]

print(f"{'Technique':<28}{'Seconds':>10}{'Row count':>15}")
for technique, seconds, row_count in comparison:
    print(f"{technique:<28}{seconds:>10.1f}{row_count:>15}")

# COMMAND ----------

# MAGIC %md
# MAGIC **Key takeaways:**
# MAGIC - Data skew causes long-running tasks, memory spills, and uneven resource use; it is usually caused by joining or aggregating on a key that is not uniformly distributed.
# MAGIC - Start with AQE: it is on by default in Databricks, requires no code changes, and handles moderate skew and small-partition coalescing well.
# MAGIC - Repartitioning and shuffle partition tuning are quick, low-effort levers for moderate skew, but neither removes the hot key itself.
# MAGIC - Salting is the highest-effort fix but the most effective one for severe skew, since it actually breaks up the hot key rather than just redistributing work around it.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Cleanup
# MAGIC
# MAGIC Drops the `ch_skewed_orders` sample table created in Setup.

# COMMAND ----------

spark.sql(f"DROP TABLE IF EXISTS {table_name}")

print(f"Cleanup complete: {table_name} dropped")

# COMMAND ----------

# MAGIC %md
# MAGIC **Notes:**
# MAGIC - The sample dataset's row counts are scaled down from the article's tens-of-millions-of-rows example so the notebook runs comfortably on a single node, while keeping a similar skew ratio between the largest and smallest customer group.
# MAGIC - Section 2 deliberately disables Adaptive Query Execution to establish a baseline; this is only for the comparison in this notebook and is not a recommended production setting, since AQE is enabled by default in Databricks. On serverless compute the AQE toggles are read-only (AQE is always on), so those cells leave the setting at its default rather than changing it.
# MAGIC - The partition-size check in section 1 uses `spark_partition_id()` to count rows per Spark partition, which works on both classic and serverless compute.
# MAGIC - The Spark UI checks in section 1 (Stages tab, Shuffle Read Size column) are described inline rather than executed, since that view only exists once a job runs on a live cluster.
# MAGIC - Sample data uses the fictional company CH Enterprise, stored in `testing.default`.
