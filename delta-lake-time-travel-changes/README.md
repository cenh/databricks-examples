**Article:** [How Delta Lake Time Travel and VACUUM Retention Now Work in Databricks](https://medium.com/@cralle/important-changes-coming-to-delta-lake-time-travel-databricks-december-2025-644b6fd03d9e?sk=2a5512a5842cf798fe00d4a884d55997)

# How Delta Lake Time Travel and VACUUM Retention Now Work in Databricks

Starting December 2025, Databricks strictly bounds Delta Lake time travel by the table property `delta.deletedFileRetentionDuration`, and `VACUUM`'s `RETAIN n HOURS` argument stops having any effect except for the special case `RETAIN 0 HOURS`. Together these changes make time travel and cleanup predictable: one property controls both, instead of the old behavior where how far back you could travel quietly depended on when `VACUUM` last ran.

The notebook creates a CH Enterprise sample orders table, writes several versions to it (insert, update, delete, merge, update), then walks through `DESCRIBE HISTORY`, time travel by version number and by timestamp, the retention table properties, how those properties bound time travel, and how `VACUUM` now interacts with all of it, before cleaning up.

## Files

- `delta_lake_time_travel_changes_notebook.py` - Databricks notebook (Python/SQL) covering the full pattern: versioned setup writes, `DESCRIBE HISTORY`, `VERSION AS OF` / `TIMESTAMP AS OF` time travel, retention table properties, the new `VACUUM` retention behavior, and cleanup.

## Requirements

- Unity Catalog enabled workspace
- `CREATE CATALOG` and `CREATE SCHEMA` privileges (or an existing `testing.default` catalog/schema you can create tables in)
- A Databricks Runtime version current enough to reflect the December 2025 time travel and `VACUUM` retention changes; behavior on older runtimes may still follow the pre-change rules

## Setup

The notebook's own Setup section creates `testing.default.ch_enterprise_orders` and writes five versions to it (an initial load, a status update, a delete, a merge, and a closing update), so there is real multi-version history to time travel across for the rest of the notebook. No manual setup is required beyond having a workspace and catalog/schema access as described above.

## Cleanup

The notebook's Cleanup section drops `testing.default.ch_enterprise_orders` to leave the workspace as it found it.

## Notes

The notebook runs end to end on serverless compute. Two behaviors from the article are shown as reference-only markdown rather than executed:

- The elapsed-time-based retention behavior (a query failing because it's older than `delta.deletedFileRetentionDuration`) cannot be reproduced synchronously in a single notebook run, since it depends on real wall-clock time passing. The notebook says so directly rather than faking that result.
- The `VACUUM ... RETAIN 0 HOURS` demonstration requires lowering the `spark.databricks.delta.retentionDurationCheck.enabled` Spark configuration, which serverless compute does not allow. The code is included as a reference block to run on classic compute.
