**Article:** [Declarative Automation Bundles: Creating Unity Catalog Catalogs and Schemas](https://medium.com/@cralle/creating-catalogs-and-schemas-with-databricks-asset-bundles-1ab0889b4803?sk=4381bc127972f517884328dd763c7f1d)

# Declarative Automation Bundles: Creating Unity Catalog Catalogs and Schemas

A walkthrough of declaring Unity Catalog catalogs and schemas as `resources` inside a Databricks Asset Bundle's `databricks.yml`, instead of creating them by hand with SQL/the UI or managing them separately with Terraform. Catalog and schema bundle resources require the direct deployment engine (Databricks CLI 0.287.0+), so they live alongside the jobs, pipelines, and grants a bundle already deploys.

The notebook shows the bundle YAML for creating a catalog, creating a schema inside it, and referencing that schema from a pipeline, then runs the equivalent SQL DDL (`CREATE CATALOG`, `CREATE SCHEMA`, `SHOW CATALOGS`/`SHOW SCHEMAS`) so the state a real `databricks bundle deploy` would produce can be verified directly in a workspace. The matching `GRANT` statements are shown as illustrative SQL rather than run, since they target placeholder principals that will not exist in a reader's workspace. A dedicated section covers the CLI commands (`validate` / `deploy` / `destroy`) that have to be run manually from a terminal, since they cannot execute inside a notebook cell.

## Files

- `catalogs_schemas_asset_bundles_notebook.py` - Databricks notebook (SQL/Python) covering the bundle YAML for catalog, schema, and pipeline resources, the equivalent runnable SQL DDL and grants, verification, manual CLI setup, and cleanup.

## Requirements

- Unity Catalog enabled workspace, with a metastore attached
- Privilege to create catalogs at the metastore level (metastore admin, or `CREATE CATALOG` granted explicitly)
- Databricks CLI 0.287.0 or later, only needed for the bundle YAML/CLI portions (not for running the notebook's SQL cells)

## Setup

The notebook creates its own demo catalog (`ch_enterprise_bundles`) and schema (`analytics`) as part of the examples, since the topic is specifically about creating those objects. Anything else in the notebook defaults to the `testing.default` catalog/schema.

## Manual setup required

`databricks bundle deploy`, `databricks bundle validate`, and `databricks bundle destroy` are local-CLI operations and cannot run inside a notebook cell. To actually deploy the bundle YAML shown in the notebook:

1. Install Databricks CLI 0.287.0 or later (`databricks -v` to check) and authenticate (`databricks auth login`, or an existing profile in `~/.databrickscfg`).
2. Create a bundle project on disk with a `databricks.yml` combining the `resources.catalogs`, `resources.schemas`, and `resources.pipelines` blocks from the notebook, under a `bundle:` name and a `targets:` section for the environment (for example `dev`).
3. From the project root, run:
   ```bash
   databricks bundle validate -t dev
   databricks bundle deploy -t dev
   databricks bundle summary -t dev   # optional, inspect what was deployed
   databricks bundle destroy -t dev   # tears everything back down
   ```

None of these commands are run against a live workspace as part of this repo; only the notebook's SQL cells are runnable as-is.

## Cleanup

The notebook's own cleanup cell drops the demo objects it created directly via SQL:

```sql
DROP SCHEMA IF EXISTS ch_enterprise_bundles.analytics CASCADE;
DROP CATALOG IF EXISTS ch_enterprise_bundles CASCADE;
```

If you also ran `databricks bundle deploy` from the Manual setup required steps above, tear that down separately with `databricks bundle destroy -t dev`; bundle-managed resources are not removed by dropping them via SQL alone.
