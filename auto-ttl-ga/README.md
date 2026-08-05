**Article:** [Auto-TTL in Databricks: Automatic Row Deletion That Actually Works](https://medium.com/@cralle/auto-ttl-in-databricks-automated-data-retention-done-properly-5ea511b45c1d?sk=105895af20d93c0c0cd4b507029229e2)

# Auto-TTL in Databricks: Automatic Row Deletion That Actually Works

**Auto-TTL** is a table-level retention policy that automatically deletes rows a set number of days after a timestamp column you choose, no cron jobs or cleanup DAGs required. Predictive Optimization handles the deletion, purging, and VACUUM in the background. This notebook covers setting a policy at creation time and on an existing table, verifying and removing a policy, reading from an Auto-TTL table with Structured Streaming, and monitoring Auto-TTL activity and cost through system tables.

## Files

- `auto_ttl_ga_notebook.py` — Databricks notebook (SQL + PySpark) covering setup, verification, streaming reads, monitoring via system tables, policy removal, and cleanup.

## Requirements

- Predictive Optimization enabled on the catalog, schema, or table
- Databricks Runtime 17.3 or above to set Auto-TTL policies (DBR 17.2 and below can still read/write tables with Auto-TTL configured)
