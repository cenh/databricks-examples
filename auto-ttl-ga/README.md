**Article:** [Auto-TTL in Databricks: Automatic Row Deletion That Actually Works](https://medium.com/@cralle/auto-ttl-in-databricks-automated-data-retention-done-properly-5ea511b45c1d?sk=105895af20d93c0c0cd4b507029229e2)

# Auto-TTL in Databricks: Automatic Row Deletion That Actually Works

**Auto-TTL** is a table-level retention policy that automatically deletes rows a set number of days after a timestamp column you choose, no cron jobs or cleanup DAGs required. Predictive Optimization handles the deletion, purging, and VACUUM in the background. This notebook covers setting a policy at creation time and on an existing table, verifying and removing a policy, reading from an Auto-TTL table with Structured Streaming, monitoring Auto-TTL activity and cost through system tables, and the streaming-table form of the policy in Lakeflow Spark Declarative Pipelines.

## Files

- `auto_ttl_ga_notebook.py` - Databricks notebook (SQL + PySpark) covering setup, verification, streaming reads, monitoring via system tables, policy removal, streaming-table policies in pipeline code, and cleanup.

## Requirements

- Predictive Optimization enabled on the catalog, schema, or table
- Databricks Runtime 17.3 or above, or serverless, to set Auto-TTL policies (DBR 17.2 and below can still read/write tables with Auto-TTL configured)

## Manual setup required

Section 7 covers Auto-TTL on **streaming tables**, and none of it can run inside a notebook cell. A
streaming table is defined inside a pipeline, and the `@dp.table` decorator only means anything when
the file is evaluated as pipeline source code, so those cells are reference-only markdown rather than
executable cells. To run them for real:

1. Create a Lakeflow Spark Declarative Pipeline and point it at a source file (Python or SQL).
2. Copy the `CREATE STREAMING TABLE ... DELETE ROWS 30 DAYS AFTER event_time` statement, or the
   `@dp.table(auto_ttl={"timestamp_column": "event_time", "expire_in_days": 30})` decorator, into that
   file. Substitute your own catalog, schema, and source table for `testing.default.raw_events`.
3. Run the pipeline. The streaming table is created with the retention policy attached.
4. To change or remove the policy, edit the pipeline source and republish. `ALTER TABLE ... DROP ROW
   DELETION` and `ALTER STREAMING TABLE` cannot modify Auto-TTL on a streaming table; setting
   `auto_ttl=None` in the decorator removes it.

Anything reading downstream of a streaming table with Auto-TTL needs `skipChangeCommits = true` on
the streaming read, for the same reason as section 4: the deletions arrive as data changes.

## Setup

Run the notebook top to bottom on serverless (or a DBR 17.3+ cluster). It creates its own sample tables in `testing.default` (change the catalog and schema in the cells if you use different names), sets and verifies Auto-TTL policies, runs a Structured Streaming read, then drops everything it created in the final Cleanup cells so the notebook is safe to re-run.
