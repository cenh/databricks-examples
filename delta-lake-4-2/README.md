**Article:** [Delta Lake 4.2: VARIANT GA, SQL Schema Evolution, and Atomic RTAS for Catalog-Managed Tables](https://medium.com/@cralle/what-developers-need-to-know-about-delta-lake-4-2-1c2b73dd2747?sk=a4d071df6d4b39083e2a28ebb447940e)

# Delta Lake 4.2: VARIANT GA, SQL Schema Evolution, and Atomic RTAS for Catalog-Managed Tables

A SQL and PySpark walkthrough of what actually changed in Delta Lake 4.2: `INSERT ... BY NAME` finally participates in automatic schema evolution, VARIANT and variant shredding go GA, RTAS and dynamic partition overwrite become truly atomic on catalog-managed tables, UniForm's Iceberg metadata generation goes synchronous, and Delta Spark V2 streaming picks up the read options production jobs actually use.

The notebook builds one continuous clickstream pipeline for a fictional company, CH Enterprise, bronze table to evolving silver table to streaming gold table, and demonstrates each feature against it in order, then drops everything it created.

## Files

- `delta_lake_4_2_notebook.py` - Databricks notebook (SQL/Python) covering schema evolution from pure SQL, VARIANT GA with shredding, atomic RTAS and dynamic partition overwrite, synchronous UniForm, Delta Spark V2 streaming and CDF fixes, collations, and setup/cleanup for all of it.

## Requirements

- Unity Catalog enabled workspace
- Databricks Runtime 17.0+ (or the equivalent serverless compute version) for Delta Lake 4.2 behavior
- `CREATE TABLE` and `CREATE VOLUME` privileges on the `testing.default` schema

## Setup

Run the notebook's Setup cells first. They create:

- `testing.default.clickstream_raw`, a bronze table with `device_type` and a `raw_properties` JSON string column already populated
- `testing.default.clickstream`, a silver table that starts out without `device_type` or `properties`, so the rest of the notebook has real columns to evolve into it, with Change Data Feed enabled from creation for the CDF section
- a managed volume, `testing.default.checkpoints`, for the streaming checkpoint used later in the notebook

Every other table used in the notebook (`clickstream_curated`, `clickstream_daily_summary`, `clickstream_events_ci`) is created inline in the section that needs it.

## Manual setup required

None. Every feature covered runs as SQL or PySpark against sample tables created in Setup. The geospatial column type and the new Apache Flink connector in the final section are shown as reference code rather than executed, since geospatial columns are new at the protocol level and not available on every workspace yet, and the Flink connector configures an external Flink job rather than anything a Databricks notebook runs; this is a version/Preview caveat, not a manual setup step.

The notebook is written to run on serverless compute, which does not allow setting the `spark.databricks.delta.schema.autoMerge.enabled` or `spark.sql.sources.partitionOverwriteMode` session configs. The schema-evolution and dynamic-partition-overwrite sections therefore use the equivalent DataFrame write options (`.option("mergeSchema", "true")` and `.option("partitionOverwriteMode", "dynamic")`); the pure-SQL form is shown alongside as reference. The `delta.stats.skipping.forceOptimizeStatsCollection` table property is likewise shown for reference only, as its name is runtime/version dependent and is not recognized on every runtime.

## Cleanup

The notebook's Cleanup section drops every table it created (`clickstream_events_ci`, `clickstream_curated`, `clickstream_daily_summary`, `clickstream`, `clickstream_raw`) and drops the `checkpoints` volume.
