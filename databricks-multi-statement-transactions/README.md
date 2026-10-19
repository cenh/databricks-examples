**Article:** [Databricks Multi-Statement Transactions Are GA: All Tables or None](https://medium.com/@cralle/databricks-multi-statement-transactions-5175d2c17dd1?sk=e5fb728f104ca1b20ea270b5e0f719cf)

# Databricks Multi-Statement Transactions: All Tables or None

A hands-on look at multi-statement transactions on Databricks, Generally Available for Unity Catalog managed Delta tables since July 22, 2026 (announced on the Databricks blog on October 16): several SQL statements, across several Unity Catalog managed Delta tables, commit together or roll back together. The notebook covers both modes, the non-interactive `BEGIN ATOMIC ... END` block and the interactive `BEGIN TRANSACTION` session ended with `COMMIT` or `ROLLBACK`. The docs still list writing to managed Iceberg tables inside a transaction as Private Preview, so the notebook uses Delta.

It follows a CH Enterprise order batch that writes three tables: `ch_orders`, `ch_inventory`, and `ch_load_log`. A write-audit-publish block publishes one batch, and the next three sections try to break it: a simulated crash between two writes, a batch that fails its oversell audit, and a rerun stopped by the idempotency audit. Each one leaves every table unchanged. It ends with the Delta history, where each batch is a single `TRANSACTION` version per table, and an interactive transaction rolled back by hand.

## Files

- `multi_statement_transactions_demo.py` - Databricks notebook (SQL) covering catalog commits on new tables and on an existing one (`ALTER TABLE`), `DESCRIBE DETAIL` to confirm `catalogManaged`, a `BEGIN ATOMIC` write-audit-publish block with `SIGNAL` audits and `GET DIAGNOSTICS ... = ROW_COUNT`, a simulated mid-block crash with automatic rollback, a rejected oversell batch, an idempotent rerun, `DESCRIBE HISTORY` showing one commit per table, an interactive `BEGIN TRANSACTION` with `ROLLBACK`, and cleanup.

## Requirements

- A SQL warehouse to run every section. Sections 1 to 6 also run on serverless compute or on a cluster with Databricks Runtime 18 LTS or above; section 7 (interactive transactions) runs only on a SQL warehouse.
- On a cluster, Databricks Runtime 18 LTS, not 18.0 to 18.2: `BEGIN ATOMIC` runs on 18.0 and above, but `GET DIAGNOSTICS ... = ROW_COUNT` fails with `PARSE_SYNTAX_ERROR` before 18 LTS.
- Unity Catalog managed Delta tables with catalog commits (`'delta.feature.catalogManaged' = 'supported'`). The notebook creates its tables this way and upgrades one existing table with `ALTER TABLE`. Every job that reads or writes those tables needs Databricks Runtime 16.4 or above.
- `USE CATALOG` on `testing`, and `USE SCHEMA` and `CREATE TABLE` on `testing.default`, or a catalog and schema of your own (see How to run).

The notebook was validated on a serverless SQL warehouse (all sections), on serverless compute (sections 1 to 6), and on a Databricks Runtime 18 LTS cluster (sections 1 to 6).

## How to run

1. Import `multi_statement_transactions_demo.py` into your Databricks workspace (**Workspace**, **Import**, or `databricks workspace import`).
2. If you do not use `testing.default`, change the `USE CATALOG` and `USE SCHEMA` cell at the top. Every other cell uses unqualified table names.
3. Attach the notebook to a SQL warehouse. On serverless compute or a cluster, run sections 1 to 6 and skip section 7.
4. Run the cells in order. One cell each in sections 3, 4, and 5 is expected to fail, and each has an **Expected to fail** note right above it. They stop with `[USER_RAISED_EXCEPTION]` (SQLSTATE 45000) and the messages `Simulated crash after the orders insert`, `Batch would oversell inventory`, and `Batch already published`. The check cell after each one shows that nothing was written. "Run all" stops at the first of them, so click **Run all below** on the next cell to continue.
5. Run the two cells of section 7 back to back. The first opens a transaction and the second rolls it back, and an interactive transaction rolls back by itself after 10 idle minutes.

What to expect: after section 2, 3 orders, a load log row for `b-001` with `orders_loaded = 3`, and stock at 17 desks, 3 chairs, and 7 monitor arms. The checks after sections 3, 4, and 5 show those numbers unchanged. `DESCRIBE HISTORY ch_inventory` in section 6 shows version 0 `CREATE TABLE`, version 1 `WRITE`, and version 2 `TRANSACTION`, with the `MERGE` listed under `intermediateOperationParameters`. Section 7 shows 13 chairs inside the transaction and 3 after `ROLLBACK`.

## Cleanup

The Cleanup cell drops the four tables the notebook creates: `ch_orders`, `ch_inventory`, `ch_load_log`, and `ch_orders_staging`. The setup cell in section 1 drops the same four first, so the notebook is safe to rerun, but it also removes any other table with those names. Run it in a schema where those names are free: the [`abac-cross-engine-views`](../abac-cross-engine-views) example in this repo also creates a `ch_orders` table in `testing.default`.
