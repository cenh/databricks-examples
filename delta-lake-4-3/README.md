**Article:** [Delta Lake 4.3: Selective Data Replacement and the Unity Catalog Delta APIs](https://medium.com/@cralle/delta-lake-4-3-selective-data-replacement)

# Delta Lake 4.3: Selective Data Replacement and the Unity Catalog Delta APIs

A PySpark and SQL walkthrough of the selective overwrite primitives added in Delta Lake 4.3: `replaceUsing` replaces every target row whose key columns match the source, and `replaceOn` takes a boolean condition instead, so NULL-safe matching with `<=>` becomes possible. Both are a better fit than `replaceWhere` for backfills and late-arriving data, because there is no predicate string to keep in sync with the data and an empty source deletes nothing.

The notebook uses a sample company, CH Enterprise, and its daily order snapshots. It builds an `orders` table and an `orders_updates` batch, replaces a single day and region with `replaceUsing`, shows the same operation in SQL with `REPLACE USING`, walks through why plain equality leaves a duplicate when a match column is NULL, fixes it with `replaceOn`, shows that an empty source is a no-op, and demonstrates the combinations Delta rejects, then drops everything it created.

## Files

- `delta_lake_4_3_notebook.py` - Databricks notebook (Python/SQL) covering `replaceUsing`, the SQL `REPLACE USING` form, NULL matching pitfalls, NULL-safe `replaceOn` with `targetAlias`, empty-source no-op behavior, and the constraints that reject combining these options with `replaceWhere`, `partitionOverwriteMode`, or `overwriteSchema`.

## Requirements

- Unity Catalog enabled workspace
- Databricks Runtime 18.2+ (or the equivalent serverless compute version) for the Python and Scala `replaceUsing` / `replaceOn` DataFrame options. The SQL forms `REPLACE USING` and `REPLACE ON` are available earlier, from Databricks Runtime 16.3 and 17.1 respectively.
- `CREATE TABLE` privileges on the `testing.default` schema

## Setup

Run the notebook's first cell to set the catalog and schema, then Section 1 to create the sample tables. It creates:

- `testing.default.orders`, the target table with three rows across two dates and two regions
- `testing.default.orders_updates`, a corrected batch for one day and region plus one order for a region the target has never seen

Sections 4 and 5 recreate both tables with a NULL region on each side to demonstrate the NULL matching behavior, so the tables are self-contained and no manual data loading is needed.

## Manual setup required

None. Every section runs as PySpark or SQL against the sample tables created in the notebook. Two things are worth knowing but are not manual steps:

- The DataFrame `replaceUsing` and `replaceOn` options require Databricks Runtime 18.2 or later. On 16.3 through 17.1 only the SQL `REPLACE USING` / `REPLACE ON` forms are available, and dynamic data overwrite requires a partitioned table with the full set of partition columns; the unpartitioned and liquid-clustered support arrives in 17.2. Check your runtime before refactoring a pipeline.
- The final section in the constraints part intentionally triggers a rejected combination (`replaceUsing` with `replaceWhere`) and catches the exception to print it. This is expected behavior, not a failure.

## Cleanup

The notebook's final section drops both tables it created (`testing.default.orders` and `testing.default.orders_updates`).
