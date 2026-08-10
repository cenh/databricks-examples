**Article:** [Managing Unity Catalog External Locations with Declarative Automation Bundles](https://medium.com/@cralle/managing-unity-catalog-external-locations-with-declarative-automation-bundles-f754263b74eb?sk=16d20730327139e996f9f4884bee71c2)

# Managing Unity Catalog External Locations with Declarative Automation Bundles

As of Databricks CLI version 0.289.0, you can declare Unity Catalog external locations directly inside a Declarative Automation Bundle's `databricks.yml`, instead of managing them through the Databricks REST API or a separate Terraform project. This notebook walks through the bundle YAML for a basic external location, inline grants, and a production-ready example with file event queues, then covers the SQL side: verifying storage credentials and external locations, creating one, describing it, granting privileges on it, and cleaning it up. It also compares bundles to Terraform for this use case and notes when Terraform is still the better fit.

## Files

- `unity_catalog_external_locations_notebook.py` - Databricks notebook (SQL + PySpark) covering the bundle YAML for external locations, the required manual/CLI steps to actually deploy one, SQL DDL for creating and managing an external location, grants, a bundles-vs-Terraform comparison, and cleanup.

## Requirements

- Unity Catalog enabled workspace with a metastore attached
- `CREATE EXTERNAL LOCATION` privilege on the metastore (metastore admins have this by default)
- Databricks CLI version 0.289.0 or above, and the direct deployment engine, to deploy `external_locations` as a bundle resource (only needed for the manual/CLI steps, not for running the notebook itself)
- A cloud storage container/bucket and a cloud identity (IAM role, managed identity, or service account) able to access it, for anything beyond the read-only verification cells

## Setup

The notebook sets `testing` / `default` as the active catalog and schema for anything scoped to a catalog/schema. External locations and storage credentials are metastore-level objects in Unity Catalog, so most of the notebook's SQL and PySpark cells operate at the metastore level rather than inside a specific catalog or schema. No sample tables are needed for this topic; the "sample data" here is the external location and storage credential objects themselves, described below.

## Manual setup required

Two things in this topic cannot run inside a notebook cell, and the notebook flags this clearly wherever it applies:

1. **`databricks bundle deploy`** is a Databricks CLI command that reads a local `databricks.yml` and talks to the Databricks REST API from outside the workspace. There is no way to invoke it from a notebook cell. The notebook shows the bundle YAML and the exact CLI commands (`databricks bundle validate`, `databricks bundle deploy --target dev`, `databricks bundle destroy --target dev`) as reference, not as runnable cells.
2. **Creating a real storage credential** requires a cloud identity (an AWS IAM role, an Azure managed identity or service principal, or a GCP service account) that already has access to the target storage container or bucket. A fresh workspace and a fresh cloud subscription do not have this by default. See the notebook's "Manual setup required" section for the full prerequisites, the `databricks storage-credentials create` command, and how to enable the direct deployment engine (`bundle: engine: direct` in `databricks.yml`, or `DATABRICKS_BUNDLE_ENGINE=direct`).

The notebook still includes runnable SQL DDL for the parts that work in any Unity Catalog workspace (`SHOW STORAGE CREDENTIALS`, `SHOW EXTERNAL LOCATIONS`), and a `CREATE EXTERNAL LOCATION` statement with clearly flagged placeholder values (`<YOUR_CLOUD_STORAGE_URL>`) for once a real storage credential exists.

## Cleanup

The notebook's cleanup cell drops the sample external location:

```sql
DROP EXTERNAL LOCATION IF EXISTS ch_enterprise_data_lake;
```

It does not delete the storage credential (other objects may depend on it) or destroy the bundle deployment. Those are manual steps:

```bash
databricks storage-credentials delete ch_enterprise_storage_credential
databricks bundle destroy --target dev
```
