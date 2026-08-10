**Article:** [What Developers Need to Know About Delta Lake 4.1](https://medium.com/@cralle/what-developers-need-to-know-about-delta-lake-4-1-f558abf85b10?sk=7326303f9ebc180b7e9dc7a781b61545)

# What Developers Need to Know About Delta Lake 4.1

A tour of Delta Lake 4.1.0: Apache Spark 4.1.0 support and the new versioned artifact naming, catalog-managed tables (still in preview), atomic CREATE TABLE AS SELECT, Server-Side Planning (preview), conflict-free enablement of Deletion Vectors and Column Mapping, AWS storage credentials and external locations, and the Delta Kernel / new V2 connector additions.

The notebook builds one CH Enterprise sample orders table, runs a runnable demo per feature against that data (or against a reference-only code snippet where a feature genuinely cannot run inside a notebook cell, such as real AWS storage credentials or the Delta Kernel Java/Rust API), and cleans everything up at the end.

## Files

- `delta_lake_4_1_notebook.py` - Databricks notebook (SQL + PySpark) covering Apache Spark 4.1.0 support and breaking changes, Catalog-Managed Tables, Atomic CTAS, Server-Side Planning, Conflict-Free Feature Enablement (Deletion Vectors and Column Mapping), AWS Storage Credentials and External Locations, and Delta Kernel / the new V2 Connector, with setup and cleanup cells.

## Requirements

- Unity Catalog enabled workspace with permission to create a catalog/schema (or edit the catalog and schema names in the notebook to match ones you already have).
- Databricks Runtime running Spark 4.0.1 or later, matching Delta Lake 4.1.0 (Java 17 or higher required; Spark 3.5 is no longer supported).
- Catalog-managed tables and Server-Side Planning require your Unity Catalog metastore to have those previews enabled; without that, the corresponding cells will fail with an unsupported-feature error, which is expected given their preview status.
- No external data or connectors required; the notebook generates its own sample data using literal `INSERT` statements.

## Setup

Run the Setup cells at the top of the notebook. They create the `testing.default` catalog/schema if it does not already exist, then load one CH Enterprise orders table used across the Atomic CTAS, Server-Side Planning, and Conflict-Free Feature Enablement sections. The Catalog-Managed Tables section creates its own self-contained demo table further down, since it needs specific reader/writer protocol versions from the moment it is created.

## Manual setup required

One thing in this topic cannot run inside a notebook cell, and the notebook flags this clearly where it applies:

1. **Creating a real AWS storage credential and external location** (section 6) requires an AWS IAM role that already trusts Databricks and has access to a real S3 bucket, plus metastore admin (or `CREATE STORAGE CREDENTIAL`/`CREATE EXTERNAL LOCATION`) privileges. None of this exists in a fresh workspace or AWS account by default, and it cannot be provisioned from a notebook cell. The notebook shows the `CREATE STORAGE CREDENTIAL` and `CREATE EXTERNAL LOCATION` statements as reference only, with placeholder values, and instead runs the read-only `SHOW STORAGE CREDENTIALS` / `SHOW EXTERNAL LOCATIONS` statements, which work in any Unity Catalog workspace.

## Cleanup

Run the Cleanup cells at the end of the notebook. They drop the temporary streaming view from the Catalog-Managed Tables section, then drop every table created during Setup and the numbered sections (the orders table, the customers summary table, and the catalog-managed orders table). No storage credential or external location was created, since section 6 is reference only.
