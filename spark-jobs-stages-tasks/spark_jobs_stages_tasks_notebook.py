# Databricks notebook source
# MAGIC %md
# MAGIC # How Apache Spark Really Runs Your Code: Jobs, Stages, and Tasks
# MAGIC
# MAGIC **Article:** [How Apache Spark Really Runs Your Code: Jobs, Stages, and Tasks](https://medium.com/@cralle/how-spark-really-runs-your-code-a-deep-dive-into-jobs-stages-and-tasks-2b63b135df4e?sk=fec82fc46c1f817ad7abbad55715d222)
# MAGIC
# MAGIC Author: Christian Hansen (https://medium.com/@cralle) - Published: September 16, 2025

# COMMAND ----------

# MAGIC %md
# MAGIC Spark does not "just run your code." It breaks it into a hierarchy of
# MAGIC **Jobs -> Stages -> Tasks** and schedules that hierarchy across the cluster.
# MAGIC This notebook builds a small CH Enterprise dataset and then walks through
# MAGIC each layer of that hierarchy with a runnable cell, so you can open the
# MAGIC Spark UI (attach this notebook to a cluster, then go to the cluster's
# MAGIC **Spark UI** tab, or port 4040 on the driver for a single-node session)
# MAGIC and watch jobs, stages, and tasks appear in real time as each cell runs.
# MAGIC
# MAGIC Sections:
# MAGIC 1. Setup: CH Enterprise sample data
# MAGIC 2. Actions vs. transformations (Jobs)
# MAGIC 3. Stages and shuffle boundaries
# MAGIC 4. Tasks and partitions
# MAGIC 5. Narrow vs. wide dependencies
# MAGIC 6. Reading the Spark UI DAG visualization
# MAGIC 7. Partitioning and task count (performance implications)
# MAGIC 8. Cleanup

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Setup: CH Enterprise sample data
# MAGIC
# MAGIC We create two tables in `testing.default`:
# MAGIC
# MAGIC - `ch_enterprise_orders`: 1,000,000 synthetic orders, written across 16
# MAGIC   partitions, large enough that reads, shuffles, and repartitions produce
# MAGIC   many real tasks instead of a single trivial one.
# MAGIC - `ch_enterprise_customers`: 50,000 synthetic customers, used later as the
# MAGIC   join side that triggers a shuffle.

# COMMAND ----------

import pyspark.sql.functions as F

CATALOG = "testing"
SCHEMA = "default"

spark.sql(f"CREATE SCHEMA IF NOT EXISTS {CATALOG}.{SCHEMA}")
spark.sql(f"USE CATALOG {CATALOG}")
spark.sql(f"USE SCHEMA {SCHEMA}")

# COMMAND ----------

regions = ["North America", "EMEA", "APAC", "LATAM"]
region_array = F.array(*[F.lit(r) for r in regions])

orders_df = (
    spark.range(0, 1_000_000, numPartitions=16)
    .withColumnRenamed("id", "order_id")
    .withColumn("customer_id", (F.col("order_id") % 50_000).cast("int"))
    .withColumn("region", F.element_at(region_array, (F.col("order_id") % 4 + 1).cast("int")))
    .withColumn("order_amount", F.round(F.rand(seed=42) * 1000, 2))
    .withColumn("order_date", F.date_add(F.lit("2026-01-01"), (F.col("order_id") % 180).cast("int")))
)

orders_df.write.mode("overwrite").saveAsTable(f"{CATALOG}.{SCHEMA}.ch_enterprise_orders")

customers_df = (
    spark.range(0, 50_000, numPartitions=8)
    .withColumnRenamed("id", "customer_id")
    .withColumn("customer_name", F.concat(F.lit("CH Customer "), F.col("customer_id").cast("string")))
    .withColumn("region", F.element_at(region_array, (F.col("customer_id") % 4 + 1).cast("int")))
)

customers_df.write.mode("overwrite").saveAsTable(f"{CATALOG}.{SCHEMA}.ch_enterprise_customers")

print("Tables created:")
print(f"  {CATALOG}.{SCHEMA}.ch_enterprise_orders")
print(f"  {CATALOG}.{SCHEMA}.ch_enterprise_customers")

# COMMAND ----------

# MAGIC %md
# MAGIC **Spark UI check:** the two `saveAsTable` calls above each triggered at
# MAGIC least one job (writing the DataFrame built from `spark.range`). Open the
# MAGIC **Jobs** tab now and note the job IDs already used up; the rest of this
# MAGIC notebook will keep adding to that list, so it is easiest to follow along
# MAGIC if you keep the Jobs tab open in a separate browser tab as you run cells
# MAGIC below.

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Actions vs. transformations (Jobs)
# MAGIC
# MAGIC A **job** is created whenever an **action** (`count()`, `collect()`,
# MAGIC `write`, ...) is invoked. **Transformations** (`filter()`, `withColumn()`,
# MAGIC `groupBy()`, ...) are lazy: they just extend the DAG without running
# MAGIC anything. The cell below builds a transformation chain first (no job runs
# MAGIC yet), then calls `count()` (a job runs).

# COMMAND ----------

orders = spark.table(f"{CATALOG}.{SCHEMA}.ch_enterprise_orders")

# Transformations only: lazy, nothing executes yet.
high_value_orders = (
    orders
    .filter(F.col("order_amount") > 100)
    .withColumn("order_amount_doubled", F.col("order_amount") * 2)
)

# Action: this is what actually triggers a job.
high_value_count = high_value_orders.count()
print(f"Orders with amount > 100: {high_value_count}")

# COMMAND ----------

# MAGIC %md
# MAGIC **Spark UI check:** go to the **Jobs** tab and find the job that just ran
# MAGIC (its description mentions `count`). Notice there is no separate job for
# MAGIC the `filter` or `withColumn` calls; they only show up as steps inside the
# MAGIC physical plan of this one job. Click into the job and open its
# MAGIC **Associated SQL Query** to see `filter` and `withColumn` folded into a
# MAGIC single `WholeStageCodegen` stage in the DAG visualization.

# COMMAND ----------

# Two more actions on the same lazy DataFrame: without caching, Spark
# recomputes the filter and withColumn from scratch for each one.
print("count() again:", high_value_orders.count())
print("sample rows:", high_value_orders.limit(3).collect())

# COMMAND ----------

# MAGIC %md
# MAGIC **Spark UI check:** the Jobs tab now shows two more jobs for the
# MAGIC `count()` and `collect()` calls above, in addition to the first `count()`.
# MAGIC Three actions on the same DataFrame produced three separate jobs, and the
# MAGIC `filter`/`withColumn` transformation ran again inside each one. This is
# MAGIC exactly the "redundant computation without caching" cost the article
# MAGIC calls out.

# COMMAND ----------

# MAGIC %md
# MAGIC **Caching (reference).** On classic (non-serverless) compute you can
# MAGIC materialize the transformation chain once with `cache()`, so later actions
# MAGIC reuse it instead of recomputing the filter and `withColumn` from scratch:
# MAGIC
# MAGIC ```python
# MAGIC high_value_orders.cache()
# MAGIC print("first count() after cache():", high_value_orders.count())
# MAGIC print("second count() after cache():", high_value_orders.count())
# MAGIC ```
# MAGIC
# MAGIC **Spark UI check:** compare the two jobs this produces in the Jobs tab. The
# MAGIC first one still does the full scan and filter (it is what populates the
# MAGIC cache); check the **Storage** tab afterwards and you should see
# MAGIC `high_value_orders` listed as a cached DataFrame. The second job should show
# MAGIC a much shorter duration and read from the in-memory cache instead of
# MAGIC re-scanning the table.
# MAGIC
# MAGIC This is shown as reference code because `cache()`/`persist()` are not
# MAGIC available on serverless compute (`[NOT_SUPPORTED_WITH_SERVERLESS] PERSIST
# MAGIC TABLE`); run it on a classic cluster to watch the cached-DataFrame behavior
# MAGIC in the UI.

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Stages and shuffle boundaries
# MAGIC
# MAGIC Every job is broken into **stages**, and the boundary between stages is
# MAGIC always a **shuffle**: a wide transformation (`groupBy()`, `join()`,
# MAGIC `repartition()`, ...) that has to move data across the cluster. A stage
# MAGIC is a set of tasks that can run without exchanging data with each other.

# COMMAND ----------

orders = spark.table(f"{CATALOG}.{SCHEMA}.ch_enterprise_orders")
customers = spark.table(f"{CATALOG}.{SCHEMA}.ch_enterprise_customers")

orders_with_customers = orders.join(customers, on="customer_id", how="inner")
orders_with_customers.write.format("noop").mode("overwrite").save()

# COMMAND ----------

# MAGIC %md
# MAGIC **Spark UI check:** open the **Stages** tab for the job this join
# MAGIC triggered. You should see multiple stages: one (or more) stage per input
# MAGIC DataFrame that writes a **shuffle write**, followed by a stage that does a
# MAGIC **shuffle read** from both sides and performs the actual join in a
# MAGIC `WholeStageCodegen` step. Click into the shuffle stage and look at the
# MAGIC **Shuffle Read Size / Records** and **Shuffle Write Size / Records**
# MAGIC metrics; this is the network and disk I/O cost the article means by
# MAGIC "shuffles are costly."

# COMMAND ----------

region_totals = (
    orders
    .groupBy("region")
    .agg(F.sum("order_amount").alias("total_amount"), F.count("*").alias("num_orders"))
)
region_totals.write.format("noop").mode("overwrite").save()

# COMMAND ----------

# MAGIC %md
# MAGIC **Spark UI check:** `groupBy().agg()` is another wide transformation, so
# MAGIC this job also shows a shuffle boundary: a first stage that partially
# MAGIC aggregates and shuffle-writes per region, and a second stage that
# MAGIC shuffle-reads and finishes the aggregation. Compare the number of tasks in
# MAGIC each stage; the second stage's task count is capped by the number of
# MAGIC distinct regions being aggregated into, which is exactly the kind of
# MAGIC skew the article warns about (few distinct keys means few tasks doing all
# MAGIC the work in the reduce stage).

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Tasks and partitions
# MAGIC
# MAGIC Within a stage, Spark creates one **task** per partition. Tasks are the
# MAGIC smallest unit of execution and map directly onto cores: each task
# MAGIC processes exactly one partition, and a core runs one task at a time.

# COMMAND ----------

from pyspark.sql.functions import spark_partition_id

def num_partitions(df):
    # The RDD API (df.rdd.getNumPartitions()) is not available on serverless
    # compute, so we count the distinct partition IDs the DataFrame's rows
    # actually map to instead. This triggers a small job of its own.
    return df.select(spark_partition_id()).distinct().count()

print("ch_enterprise_orders partitions:", num_partitions(orders))

orders_4_partitions = orders.repartition(4)
print("After repartition(4):", num_partitions(orders_4_partitions))
orders_4_partitions.write.format("noop").mode("overwrite").save()

# COMMAND ----------

# MAGIC %md
# MAGIC **Spark UI check:** open the Stages tab for the job above and click into
# MAGIC the final stage. The **Tasks** table at the bottom should list exactly 4
# MAGIC tasks, one per partition after `repartition(4)`. If the cluster has more
# MAGIC than 4 cores available, some of them sit idle while this stage runs;
# MAGIC that is the "too few tasks underutilize resources" case from the article.

# COMMAND ----------

orders_64_partitions = orders.repartition(64)
print("After repartition(64):", num_partitions(orders_64_partitions))
orders_64_partitions.write.format("noop").mode("overwrite").save()

# COMMAND ----------

# MAGIC %md
# MAGIC **Spark UI check:** the equivalent stage for this job should now show 64
# MAGIC tasks instead of 4. Compare the **Summary Metrics** (duration, shuffle
# MAGIC read size per task) between this stage and the previous one: more,
# MAGIC smaller tasks generally keep more cores busy, but push it too far (as in
# MAGIC Section 7 below) and scheduling overhead starts to dominate. A common
# MAGIC rule of thumb is 2 to 3 tasks per CPU core in the cluster.

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5. Narrow vs. wide dependencies
# MAGIC
# MAGIC Stage boundaries exist because of the difference between **narrow**
# MAGIC dependencies (`filter()`, `select()`, `map()`), where each output
# MAGIC partition depends on exactly one input partition and no shuffle is
# MAGIC needed, and **wide** dependencies (`groupBy()`, `join()`,
# MAGIC `repartition()`), where an output partition can depend on many input
# MAGIC partitions and Spark has to shuffle. `.explain()` shows this directly: a
# MAGIC wide dependency's physical plan contains an `Exchange` node, a narrow
# MAGIC dependency's does not.

# COMMAND ----------

narrow_plan = (
    orders
    .filter(F.col("region") == "EMEA")
    .select("order_id", "customer_id", "order_amount")
)
narrow_plan.explain(True)

# COMMAND ----------

# MAGIC %md
# MAGIC Look at the **Physical Plan** printed above: `Filter` and `Project` sit
# MAGIC directly on top of the table scan, with no `Exchange` in between. That
# MAGIC absence of `Exchange` is what makes this a narrow dependency; in the
# MAGIC Spark UI it would run as a single stage with no shuffle read/write
# MAGIC metrics.

# COMMAND ----------

wide_plan = orders.groupBy("region").agg(F.avg("order_amount").alias("avg_amount"))
wide_plan.explain(True)

# COMMAND ----------

# MAGIC %md
# MAGIC This time the **Physical Plan** contains an `Exchange hashpartitioning`
# MAGIC node between the partial aggregation and the final aggregation. That
# MAGIC `Exchange` is the shuffle, and it is exactly what shows up as a stage
# MAGIC boundary in the Spark UI when this plan actually runs as a job (see the
# MAGIC `region_totals` example in Section 3).

# COMMAND ----------

# MAGIC %md
# MAGIC ## 6. Reading the Spark UI DAG visualization
# MAGIC
# MAGIC The DAG visualization on a job's detail page draws exactly the hierarchy
# MAGIC covered in this notebook: boxes grouped into stages, arrows showing data
# MAGIC flow between them, and shuffle read/write arrows crossing stage
# MAGIC boundaries. The next cell runs a query with a join, a group by, and an
# MAGIC order by, all in one job, so there is a richer DAG to look at.

# COMMAND ----------

region_summary = (
    orders
    .join(customers, on="customer_id", how="inner")
    # Both tables carry a `region` column, so qualify which one to group by;
    # an unqualified "region" here raises AMBIGUOUS_REFERENCE.
    .groupBy(orders["region"])
    .agg(
        F.sum("order_amount").alias("total_amount"),
        F.countDistinct("customer_id").alias("distinct_customers"),
    )
    .orderBy(F.col("total_amount").desc())
)
region_summary.show()

# COMMAND ----------

# MAGIC %md
# MAGIC **Spark UI check:** open this job's detail page and click **DAG
# MAGIC Visualization**. You should be able to trace, left to right: a scan of
# MAGIC `ch_enterprise_orders` and a scan of `ch_enterprise_customers`, each
# MAGIC feeding an `Exchange` (the join shuffle), a `WholeStageCodegen` box doing
# MAGIC the join, another `Exchange` for the `groupBy`, the aggregation, and a
# MAGIC final `Exchange` for the `orderBy`. Each `Exchange` in that picture is a
# MAGIC stage boundary; count them and compare against the number of stages
# MAGIC listed for this job on the Stages tab. Also worth a look while you are
# MAGIC here: the **SQL/DataFrame** tab for this query's logical and physical
# MAGIC plans (the same Catalyst optimizations, like predicate pushdown, that
# MAGIC `.explain()` prints from code), and the **Executors** tab to see how the
# MAGIC tasks from each stage were spread across executors.

# COMMAND ----------

# MAGIC %md
# MAGIC ## 7. Partitioning and task count (performance implications)
# MAGIC
# MAGIC Partition count directly drives task count, and both extremes hurt. The
# MAGIC article makes the point with two RDD examples: too many partitions for too
# MAGIC little data, and a more balanced ratio of partitions to data volume. They
# MAGIC are shown below as reference code because the RDD API
# MAGIC (`spark.sparkContext.parallelize`) is not available on serverless compute
# MAGIC (`[NOT_IMPLEMENTED] Using custom code using PySpark RDDs is not allowed on
# MAGIC serverless compute`); run them on a classic or dedicated cluster to watch
# MAGIC the task counts in the UI. The runnable `repartition(4)` vs.
# MAGIC `repartition(64)` comparison in Section 4 makes the same task-count point
# MAGIC on serverless.

# COMMAND ----------

# MAGIC %md
# MAGIC **Too many partitions (reference).** The first example spreads 20 data
# MAGIC points across 100 partitions:
# MAGIC
# MAGIC ```python
# MAGIC too_many_partitions = spark.sparkContext.parallelize([i for i in range(20)], 100)
# MAGIC too_many_partitions.map(lambda x: x * 10).count()
# MAGIC ```
# MAGIC
# MAGIC **Spark UI check:** this job's single stage has 100 tasks for only 20
# MAGIC data points, one value per task. Open the stage detail page and look at
# MAGIC the **Scheduler Delay** and task duration columns; most of the wall clock
# MAGIC time here is scheduling overhead, not actual work, because there is
# MAGIC almost nothing for each task to do.

# COMMAND ----------

# MAGIC %md
# MAGIC **A balanced ratio (reference).** The second example spreads 10,000 data
# MAGIC points across 8 partitions:
# MAGIC
# MAGIC ```python
# MAGIC balanced_partitions = spark.sparkContext.parallelize([i for i in range(10_000)], 8)
# MAGIC balanced_partitions.map(lambda x: x * 10).count()
# MAGIC ```
# MAGIC
# MAGIC **Spark UI check:** this job's stage has only 8 tasks, each doing far more
# MAGIC work (1,250 values instead of at most 1). Compare the total stage
# MAGIC duration against the "too many partitions" example above; fewer, larger
# MAGIC tasks that still cover all available cores generally beat many tiny ones.
# MAGIC The same idea applies to the `repartition(4)` vs. `repartition(64)`
# MAGIC comparison from Section 4: the goal is roughly 2 to 3 tasks per CPU core,
# MAGIC not the largest partition count you can get away with.

# COMMAND ----------

# MAGIC %md
# MAGIC ## 8. Cleanup
# MAGIC
# MAGIC Drop the two sample tables created in Section 1. (The `cache()` example in
# MAGIC Section 2 is reference-only on serverless, so there is nothing to
# MAGIC unpersist here; on a classic cluster where you ran it, call
# MAGIC `high_value_orders.unpersist()` first.)

# COMMAND ----------

spark.sql(f"DROP TABLE IF EXISTS {CATALOG}.{SCHEMA}.ch_enterprise_orders")
spark.sql(f"DROP TABLE IF EXISTS {CATALOG}.{SCHEMA}.ch_enterprise_customers")

print("Cleanup complete.")

# COMMAND ----------

# MAGIC %md
# MAGIC **Notes:** Every "Spark UI check" cell above assumes you are running this
# MAGIC notebook attached to a cluster with the Spark UI reachable from the
# MAGIC cluster's "Spark UI" link (or port 4040 on the driver for a local
# MAGIC session). The UI itself cannot be captured from inside a notebook cell,
# MAGIC so those cells describe exactly what to click on and what you should see,
# MAGIC rather than reproducing the UI. Partition counts, table sizes, and exact
# MAGIC task counts will vary with cluster size and Databricks Runtime version;
# MAGIC the shapes described (number of stages, presence or absence of
# MAGIC `Exchange`, tasks-per-partition) should hold regardless.
