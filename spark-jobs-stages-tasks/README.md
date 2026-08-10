**Article:** [How Apache Spark Really Runs Your Code: Jobs, Stages, and Tasks](https://medium.com/@cralle/how-spark-really-runs-your-code-a-deep-dive-into-jobs-stages-and-tasks-2b63b135df4e?sk=fec82fc46c1f817ad7abbad55715d222)

# How Apache Spark Really Runs Your Code: Jobs, Stages, and Tasks

A PySpark walkthrough of Spark's execution hierarchy: how an action turns lazy transformations into a job, how the DAG Scheduler splits that job into stages at shuffle boundaries, and how each stage fans out into tasks based on partitions. The notebook builds a CH Enterprise orders and customers dataset large enough to produce real multi-partition jobs, then works through actions vs. transformations, narrow vs. wide dependencies, and partition sizing, with guidance on exactly what to look at in the Spark UI after each cell.

The notebook creates two sample tables, runs a series of triggering cells (counts, joins, group-bys, repartitions, and `.explain()` calls), and cleans everything up at the end. The `cache()` and RDD `parallelize` examples are shown as reference code rather than executed, because `cache()`/`persist()` and the RDD API are not available on serverless compute; run them on a classic or dedicated cluster to watch that behavior in the Spark UI.

## Files

- `spark_jobs_stages_tasks_notebook.py` - Databricks notebook (Python) covering the full execution hierarchy: setup, actions vs. transformations, stages and shuffle boundaries, tasks and partitions, narrow vs. wide dependencies, the Spark UI DAG visualization, partitioning and task count, and cleanup.

## Requirements

- A Databricks workspace with Unity Catalog enabled
- `CREATE SCHEMA` privilege on the `testing` catalog (or an existing `testing.default` schema)
- Access to the cluster's Spark UI (the cluster's "Spark UI" link in the Databricks UI, or port 4040 on the driver for a single-node session)

## Setup

Run the notebook's Setup section first. It creates two tables in `testing.default`:

- `ch_enterprise_orders`: 1,000,000 synthetic orders written across 16 partitions
- `ch_enterprise_customers`: 50,000 synthetic customers written across 8 partitions

These sizes are large enough that reads, shuffles, and repartitions in the later sections produce real multi-task stages, not a single trivial task.

## Manual setup required

None. Every operation described in the article, actions, transformations, joins, group-bys, repartitions, and `.explain()`, runs directly from notebook cells. The only step that cannot be automated is opening the Spark UI itself to look at the resulting jobs, stages, and tasks; each triggering cell is followed by a markdown note on exactly what to check there, since the notebook cannot capture the live UI.

## Cleanup

Run the notebook's Cleanup section at the end. It drops both sample tables (`ch_enterprise_orders` and `ch_enterprise_customers`) from `testing.default`.
