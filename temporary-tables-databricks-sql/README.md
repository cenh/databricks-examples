**Article:** [Temporary Tables in Databricks SQL: Staging Data Without Cluttering Your Catalog](https://medium.com/@cralle/temporary-tables-in-databricks-sql-a-familiar-pattern-finally-done-right-a5dcee1609a4?sk=65307b456bebc39155b04f7b05e658d8)

# Temporary Tables in Databricks SQL: Staging Data Without Cluttering Your Catalog

Databricks SQL now supports `CREATE TEMPORARY TABLE` (Public Preview): session-scoped, physical Delta tables for staging, exploratory analysis, and multi-step SQL transformations, without ever registering an object in Unity Catalog. Unlike a temporary view, a temporary table materializes its rows once and can be queried repeatedly, appended to with `INSERT`, and joined against, instead of re-running the underlying query on every reference.

The notebook creates two small CH Enterprise sample tables, then walks through creating and chaining temporary tables, session scope and naming precedence over a permanent table, a side-by-side comparison against temporary views, CTEs, and permanent tables, a full stage-clean-aggregate-merge ETL pipeline, and the feature's current Public Preview limitations (shown as reference syntax, since they are expected to fail by design), before cleaning everything up.

## Files

- `temporary_tables_databricks_sql_notebook.py` - Databricks notebook (SQL + PySpark) covering setup, creating and using temporary tables, session scope and isolation, comparison to views/CTEs/permanent tables, multi-step ETL with a `MERGE INTO`, current Public Preview limitations, and cleanup.

## Requirements

- Unity Catalog enabled workspace with `CREATE TABLE` privilege on `testing.default` (or edit the catalog/schema names in the notebook to match ones you already have)
- A SQL warehouse or serverless SQL compute; `CREATE TEMPORARY TABLE` is a Databricks SQL feature and is not available through the classic DataFrame/Spark API
- Temporary tables enabled for the workspace (Public Preview at the time of writing)

## Setup

Run the notebook top to bottom against serverless SQL compute or a SQL warehouse. The Setup cells create the `testing` catalog and `default` schema (if they do not already exist) and two permanent sample tables: `ch_raw_sales` and `ch_customers`. Sample sale dates are generated relative to `current_date()`, so the "last 7 days" filters used throughout the notebook always return a realistic mix of rows regardless of when it is run.

## Cleanup

Run the notebook's Cleanup cells to drop the temporary tables and view created during the demo (`tmp_sales`, `tmp_new_sales`, `tmp_enriched`, `tmp_conflict_demo`, `tmp_stage`, `tmp_clean`, `tmp_agg`, `v_sales_by_region`), and the permanent tables created in Setup and the ETL section (`ch_raw_sales`, `ch_customers`, `testing.default.tmp_conflict_demo`, `ch_sales_summary`). Temporary tables are dropped with `DROP TEMPORARY TABLE` (a plain `DROP TABLE` on an unqualified name resolves to a permanent table and errors when a temporary table of the same name exists). Dropping the temporary tables explicitly is optional, they disappear automatically when the session ends or after seven days, whichever comes first, but doing so keeps the session tidy while iterating.
