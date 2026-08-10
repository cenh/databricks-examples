**Article:** [Databricks Predictive Optimization: How Automatic OPTIMIZE, VACUUM, and CLUSTER BY AUTO Really Work](https://medium.com/@cralle/demystifying-predictive-optimization-in-databricks-automate-table-tuning-with-confidence-a5bc293292c3?sk=026c78ada316bfc19b45f7f8f555b9bc)

# Databricks Predictive Optimization: How Automatic OPTIMIZE, VACUUM, and CLUSTER BY AUTO Really Work

A walkthrough of how Predictive Optimization decides when to run `OPTIMIZE`, `ANALYZE`, and `VACUUM` on Unity Catalog managed tables, and how `CLUSTER BY AUTO` (Automatic Liquid Clustering) picks and re-evaluates clustering columns, based on a DAIS 2025 session on the topic plus Databricks documentation. Predictive Optimization itself runs asynchronously in the background rather than from notebook code, so this notebook focuses on everything that is genuinely runnable: creating a realistic file layout, setting the `ENABLE`/`DISABLE`/`INHERIT` and `CLUSTER BY AUTO` states it reads, running the manual equivalents for comparison, and querying the system tables it reports into.

The notebook builds two CH Enterprise sample tables (an orders table written as many small files, and an events table), works through each Predictive Optimization concept against them in order, and drops everything it created at the end.

## Files

- `predictive_optimization_notebook.py` - Databricks notebook (SQL/Python) covering enabling Predictive Optimization at account/catalog/schema/table level, the OPTIMIZE and VACUUM ROI model, CLUSTER BY AUTO, automatic statistics, monitoring via system tables, cost/DBU tracking, limitations, and setup/cleanup for all of it.

## Requirements

- Unity Catalog enabled workspace
- `CREATE TABLE` privilege on the `testing.default` schema
- Metastore admin or catalog owner privilege to run the `ALTER CATALOG ... PREDICTIVE OPTIMIZATION` statements in Section 1
- `system` catalog access (`storage` and `billing` schemas) for the monitoring queries in Sections 6 and 7; most workspaces have the core system schemas enabled by default

## Setup

The notebook creates its own sample tables in `testing.default`:

- `ch_enterprise_orders`, an orders table intentionally written out as roughly 40 small files, so there is a realistic layout for `OPTIMIZE` and Predictive Optimization to compact
- `ch_enterprise_events`, an events table used for the `CLUSTER BY AUTO` section

`ch_enterprise_events_clustered` is created inline in the `CLUSTER BY AUTO` section.

## Manual setup required

Predictive Optimization has been enabled by default for every existing Databricks account since May 7th, 2025, so most workspaces need no action here. If it was previously turned off at the account level, only an account admin can turn it back on, and only from the account console, this cannot be done from SQL, PySpark, or the workspace UI:

1. Sign in to the account console as an account admin.
2. Go to **Settings**, then the **Feature enablement** tab.
3. Find **Predictive optimization** and toggle it on for the account.

Everything else in the notebook, catalog/schema/table level `ENABLE`/`DISABLE`/`INHERIT`, `CLUSTER BY AUTO`, the manual `OPTIMIZE`/`ANALYZE`/`VACUUM` equivalents, and the system table queries, is real, runnable SQL/PySpark that does not require the account console.

## Cleanup

The notebook's Cleanup section resets the `ch_enterprise_orders` table's Predictive Optimization override back to `INHERIT`, then drops all three tables it created (`ch_enterprise_orders`, `ch_enterprise_events`, `ch_enterprise_events_clustered`).
