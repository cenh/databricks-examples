**Article:** [How to Fix Data Skew in Apache Spark and Databricks: AQE, Repartitioning, and Salting](https://medium.com/@cralle/handling-data-skew-in-databricks-and-pyspark-7a16dc227a09?sk=d231b048f8d17f5efd89adb14c97a9cc)

# How to Fix Data Skew in Apache Spark and Databricks: AQE, Repartitioning, and Salting

A runnable walkthrough for diagnosing and fixing data skew in Apache Spark and Databricks: spotting it with partition-size and frequency checks against a genuinely skewed CH Enterprise sample dataset, then comparing four fixes head to head, Adaptive Query Execution (AQE), `repartition()`, tuning `spark.sql.shuffle.partitions`, and key salting.

The notebook creates a skewed CH Enterprise order-events table (one customer accounts for the large majority of rows), works through one numbered section per diagnosis or fix technique with a small timed demo against that table, compares the results side by side, and cleans everything up at the end.

## Files

- `data_skew_in_spark_notebook.py` - Databricks notebook (Python) covering the full pattern: setup, identifying skew (partition sizes, key frequency), a no-fix baseline, AQE, repartitioning, shuffle partition tuning, salting, a results comparison, and cleanup.

## Requirements

- Unity Catalog enabled workspace, with `CREATE TABLE` privilege on `testing.default`
- Any current Databricks Runtime, or serverless compute; Adaptive Query Execution and its skew-join handling are enabled by default, so no specific runtime version is required
- Enough capacity to hold roughly 2.6 million rows during the join and salting demos (comfortable on a small single-node cluster or serverless)
- Note for serverless compute: the AQE toggles in sections 2 and 3 are read-only there (AQE is always on), so those cells leave the setting at its default instead of changing it; every other technique runs unchanged

## Setup

Run the Setup cells first. They create one managed Delta table under `testing.default`:

- `ch_skewed_orders` - CH Enterprise order events, partitioned by `customer_id`. One customer, `CUST-DOMINANT`, holds roughly 2,000,000 of the 2,625,000 total rows, while four other customers share the rest, so the skew is real rather than simulated.

The Setup cells also create `df_customer_lookup`, a small in-memory lookup DataFrame used as the join target in every section.

## Manual setup required

None. Every technique in this article (AQE settings, `repartition()`, `spark.sql.shuffle.partitions`, salting) is a Spark configuration or DataFrame operation, so the notebook runs end to end without any manual setup beyond having a workspace with Unity Catalog and `CREATE TABLE` privilege on `testing.default`. The Spark UI checks described in section 1 (Stages tab, Shuffle Read Size column) only exist once a job runs on a live cluster, so they are described inline rather than executed.

## Cleanup

Run the Cleanup cell at the end of the notebook to drop the `ch_skewed_orders` table from `testing.default`.
