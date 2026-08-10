# Databricks notebook source
# MAGIC %md
# MAGIC # Declarative Automation Bundles: Creating Unity Catalog Catalogs and Schemas
# MAGIC
# MAGIC **Article:** [Declarative Automation Bundles: Creating Unity Catalog Catalogs and Schemas](https://medium.com/@cralle/creating-catalogs-and-schemas-with-databricks-asset-bundles-1ab0889b4803?sk=4381bc127972f517884328dd763c7f1d)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Overview
# MAGIC
# MAGIC Databricks Asset Bundles (DABs) can now declare Unity Catalog **catalogs** and
# MAGIC **schemas** directly as bundle `resources`, instead of creating them by hand with
# MAGIC SQL/the UI, or managing them separately with Terraform. That means a catalog or
# MAGIC schema can live in `databricks.yml` next to the jobs, pipelines, and workflows that
# MAGIC depend on it, version-controlled and deployed together with the rest of the data
# MAGIC product.
# MAGIC
# MAGIC Catalog and schema resources in bundles require the **direct deployment engine**,
# MAGIC which needs **Databricks CLI 0.287.0 or later**. `databricks bundle deploy` and
# MAGIC `databricks bundle destroy` are local CLI operations, not something you can run from
# MAGIC a notebook cell, so this notebook is split into two kinds of content:
# MAGIC
# MAGIC - **Bundle YAML** shown as fenced code blocks (illustrative, run from a terminal via
# MAGIC   the CLI, see **Manual setup required** below).
# MAGIC - **Equivalent SQL DDL**, runnable directly in this notebook, that produces the same
# MAGIC   catalog/schema/grant state a bundle deploy of that YAML would create, so you can
# MAGIC   inspect and verify the result without leaving the notebook.
# MAGIC
# MAGIC The scenario used throughout: CH Enterprise wants to stand up a `ch_enterprise_bundles`
# MAGIC catalog with an `analytics` schema for one of its data products, defined as bundle
# MAGIC resources rather than provisioned separately in Terraform.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Setup
# MAGIC
# MAGIC This walkthrough is specifically about *creating* a new catalog and schema, so the
# MAGIC demo objects (`ch_enterprise_bundles` catalog and its `analytics` schema) are created
# MAGIC as part of the examples below rather than assumed to already exist. For anything in
# MAGIC this notebook that is not specific to that demo catalog/schema, the default working
# MAGIC catalog and schema is `testing.default`.

# COMMAND ----------

# MAGIC %sql
# MAGIC USE CATALOG testing;
# MAGIC USE SCHEMA default;

# COMMAND ----------

# MAGIC %md
# MAGIC ## Example 1: Creating a Catalog with DABs
# MAGIC
# MAGIC Inside `databricks.yml`, a catalog is declared as a bundle resource:
# MAGIC
# MAGIC ```yaml
# MAGIC resources:
# MAGIC   catalogs:
# MAGIC     ch_enterprise_bundles:
# MAGIC       name: ch_enterprise_bundles
# MAGIC       comment: Catalog created by Databricks Asset Bundles
# MAGIC       properties:
# MAGIC         purpose: "Testing"
# MAGIC       grants:
# MAGIC         - principal: data.engineer@chenterprise.com
# MAGIC           privileges:
# MAGIC             - USE_CATALOG
# MAGIC             - CREATE_SCHEMA
# MAGIC ```
# MAGIC
# MAGIC At deploy time this creates the `ch_enterprise_bundles` catalog, sets its comment and
# MAGIC custom `purpose` property, and applies the grant, all from the bundle definition.
# MAGIC
# MAGIC ### Equivalent SQL you can run now
# MAGIC
# MAGIC Custom catalog `properties` (like `purpose: Testing` above) are a bundle-resource
# MAGIC concept without a direct one-line SQL equivalent, so the cell below focuses on what
# MAGIC SQL actually can create and verify: the catalog itself and its comment.
# MAGIC
# MAGIC The bundle's `grants` block maps to a `GRANT` statement. It is shown here as
# MAGIC illustrative SQL rather than run in a cell, because the principal is a placeholder
# MAGIC specific to CH Enterprise that will not exist in your workspace; substitute a real
# MAGIC user or group before running it:
# MAGIC
# MAGIC ```sql
# MAGIC GRANT USE_CATALOG, CREATE_SCHEMA ON CATALOG ch_enterprise_bundles TO `data.engineer@chenterprise.com`;
# MAGIC ```

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE CATALOG IF NOT EXISTS ch_enterprise_bundles
# MAGIC COMMENT 'Catalog created by Databricks Asset Bundles';

# COMMAND ----------

# MAGIC %sql
# MAGIC SHOW CATALOGS LIKE 'ch_enterprise_bundles';
# MAGIC
# MAGIC SHOW GRANTS ON CATALOG ch_enterprise_bundles;

# COMMAND ----------

# MAGIC %md
# MAGIC ## Example 2: Creating a Schema in a Catalog
# MAGIC
# MAGIC Schemas are declared the same way, and can reference the catalog resource above by
# MAGIC name instead of hardcoding it:
# MAGIC
# MAGIC ```yaml
# MAGIC resources:
# MAGIC   schemas:
# MAGIC     ch_enterprise_analytics:
# MAGIC       name: analytics
# MAGIC       catalog_name: ${resources.catalogs.ch_enterprise_bundles.name}
# MAGIC       comment: Analytics schema created via DAB
# MAGIC       grants:
# MAGIC         - principal: ch_enterprise_analysts
# MAGIC           privileges:
# MAGIC             - SELECT
# MAGIC         - principal: ch_enterprise_data_engineering
# MAGIC           privileges:
# MAGIC             - CAN_MANAGE
# MAGIC ```
# MAGIC
# MAGIC Deploying this creates `analytics` inside `ch_enterprise_bundles`, and grants
# MAGIC `SELECT` to the `ch_enterprise_analysts` group and management privileges to
# MAGIC `ch_enterprise_data_engineering`.
# MAGIC
# MAGIC ### Equivalent SQL you can run now
# MAGIC
# MAGIC The schema itself is created and verified below. The bundle's `grants` map to `GRANT`
# MAGIC statements, shown here as illustrative SQL rather than run, because the groups are
# MAGIC placeholders specific to CH Enterprise that will not exist in your workspace;
# MAGIC substitute real groups before running them. `MANAGE` is the SQL grant name for the
# MAGIC object-management privilege the bundle YAML expresses as `CAN_MANAGE`:
# MAGIC
# MAGIC ```sql
# MAGIC GRANT SELECT ON SCHEMA ch_enterprise_bundles.analytics TO `ch_enterprise_analysts`;
# MAGIC GRANT MANAGE ON SCHEMA ch_enterprise_bundles.analytics TO `ch_enterprise_data_engineering`;
# MAGIC ```

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE SCHEMA IF NOT EXISTS ch_enterprise_bundles.analytics
# MAGIC COMMENT 'Analytics schema created via DAB';

# COMMAND ----------

# MAGIC %sql
# MAGIC SHOW SCHEMAS IN ch_enterprise_bundles;
# MAGIC
# MAGIC SHOW GRANTS ON SCHEMA ch_enterprise_bundles.analytics;

# COMMAND ----------

# MAGIC %md
# MAGIC Verifying the same catalog/schema state from PySpark, filtered down to the objects
# MAGIC this walkthrough created:

# COMMAND ----------

import pyspark.sql.functions as F

catalogs_df = spark.sql("SHOW CATALOGS")
catalogs_df.filter(F.col("catalog") == "ch_enterprise_bundles").display()

schemas_df = spark.sql("SHOW SCHEMAS IN ch_enterprise_bundles")
schemas_df.filter(F.col("databaseName") == "analytics").display()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Example 3: Using the Schema in a Pipeline
# MAGIC
# MAGIC Once a schema is a bundle resource, other resources (jobs, pipelines) can reference
# MAGIC it directly, so the schema is guaranteed to exist before the pipeline that targets it
# MAGIC deploys. This example is independent of the catalog/schema created above, so it uses
# MAGIC `testing`, the default catalog for anything not specific to the
# MAGIC `ch_enterprise_bundles` demo:
# MAGIC
# MAGIC ```yaml
# MAGIC resources:
# MAGIC   pipelines:
# MAGIC     ch_enterprise_pipeline:
# MAGIC       name: ch-enterprise-pipeline-{{.unique_id}}
# MAGIC       libraries:
# MAGIC         - notebook:
# MAGIC             path: ../src/transform.ipynb
# MAGIC       development: true
# MAGIC       catalog: ${resources.schemas.ch_enterprise_test_schema.catalog_name}
# MAGIC       target: ${resources.schemas.ch_enterprise_test_schema.id}
# MAGIC   schemas:
# MAGIC     ch_enterprise_test_schema:
# MAGIC       name: ch-enterprise-test-{{.unique_id}}
# MAGIC       catalog_name: testing
# MAGIC       comment: Created by Databricks Asset Bundles
# MAGIC ```
# MAGIC
# MAGIC This is a bundle-only construct: the templated `{{.unique_id}}` name, the
# MAGIC cross-resource `${...}` references, and the deployment ordering (schema before
# MAGIC pipeline) are all resolved by `databricks bundle deploy`, not by anything that runs
# MAGIC interactively in a notebook. It is shown here as bundle YAML rather than forced to
# MAGIC run as a cell.

# COMMAND ----------

# MAGIC %md
# MAGIC ## What This Changes Architecturally
# MAGIC
# MAGIC **Before:** Terraform created catalogs and schemas, bundles created jobs and
# MAGIC pipelines, and ownership of the two was split across tools.
# MAGIC
# MAGIC **Now:** a single bundle can define the full surface of a data product. Data
# MAGIC platform teams keep defining guardrails, and application teams can own their own
# MAGIC namespace inside those guardrails.
# MAGIC
# MAGIC In practice that makes deployments more portable, more reproducible, easier to
# MAGIC promote across environments, and easier to tear down cleanly, since the catalog and
# MAGIC schema tear down alongside everything else the bundle created.
# MAGIC
# MAGIC ## When Should You Still Use Terraform?
# MAGIC
# MAGIC Bundles do not replace Terraform. Terraform is still the better fit for workspace
# MAGIC provisioning, networking, and metastore-level configuration. For catalog and schema
# MAGIC creation tied to a specific data product, though, bundles are often the cleaner
# MAGIC abstraction, since the resource lives in the same file as the workloads that use it.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Manual setup required
# MAGIC
# MAGIC Nothing in this section runs inside this notebook. `databricks bundle` commands are
# MAGIC local-CLI operations, executed from a terminal against a bundle project on disk, not
# MAGIC from a notebook cell.
# MAGIC
# MAGIC ### Prerequisites
# MAGIC
# MAGIC - **Databricks CLI 0.287.0 or later.** Check with `databricks -v`; catalog and schema
# MAGIC   bundle resources need the direct deployment engine, which that version and later
# MAGIC   enable.
# MAGIC - A CLI authentication profile configured for the target workspace (`databricks auth
# MAGIC   login` or an existing profile in `~/.databrickscfg`).
# MAGIC - A Unity Catalog metastore attached to the workspace, and enough privilege to create
# MAGIC   catalogs at the metastore level (metastore admin, or `CREATE CATALOG` granted
# MAGIC   explicitly).
# MAGIC
# MAGIC ### Bundle project layout
# MAGIC
# MAGIC A minimal project combining the three examples above looks like this on disk:
# MAGIC
# MAGIC ```text
# MAGIC ch-enterprise-bundles/
# MAGIC   databricks.yml
# MAGIC   src/
# MAGIC     transform.ipynb
# MAGIC ```
# MAGIC
# MAGIC `databricks.yml` combines the `resources.catalogs`, `resources.schemas`, and
# MAGIC `resources.pipelines` blocks shown in Examples 1 to 3, under a top-level `bundle:`
# MAGIC name and a `targets:` section for the environment(s) to deploy to (for example
# MAGIC `dev` and `prod`).
# MAGIC
# MAGIC ### CLI commands
# MAGIC
# MAGIC Run these from the bundle project's root directory:
# MAGIC
# MAGIC ```bash
# MAGIC # Validate syntax and resolve variables/references without deploying anything
# MAGIC databricks bundle validate -t dev
# MAGIC
# MAGIC # Deploy: creates/updates the catalog, schema, and pipeline, and applies grants
# MAGIC databricks bundle deploy -t dev
# MAGIC
# MAGIC # Optional: inspect what got deployed and where
# MAGIC databricks bundle summary -t dev
# MAGIC
# MAGIC # Tear down every resource this bundle deployed, including the catalog and schema
# MAGIC databricks bundle destroy -t dev
# MAGIC ```
# MAGIC
# MAGIC ### Notes and caveats
# MAGIC
# MAGIC - `databricks bundle destroy` will not drop a catalog or schema that still contains
# MAGIC   tables/data any more cleanly than a plain `DROP CATALOG` without `CASCADE` would;
# MAGIC   empty the objects first (see **Cleanup** below) or expect the destroy to fail.
# MAGIC - Re-running `databricks bundle deploy` after editing `databricks.yml` is how you
# MAGIC   change comments, properties, or grants on an already-deployed catalog/schema; there
# MAGIC   is no separate "update" command.
# MAGIC - Everything in this section is the CLI-only counterpart to the SQL DDL run earlier
# MAGIC   in this notebook; the SQL cells show the state a real `bundle deploy` of the YAML
# MAGIC   above would produce, without requiring the CLI to inspect it.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Cleanup
# MAGIC
# MAGIC Two things were created directly by SQL in this notebook: the `ch_enterprise_bundles`
# MAGIC catalog and its `analytics` schema. Drop them the same way a bundle would if it were
# MAGIC destroyed, then switch back to the default catalog/schema.

# COMMAND ----------

# MAGIC %sql
# MAGIC DROP SCHEMA IF EXISTS ch_enterprise_bundles.analytics CASCADE;
# MAGIC DROP CATALOG IF EXISTS ch_enterprise_bundles CASCADE;
# MAGIC
# MAGIC USE CATALOG testing;
# MAGIC USE SCHEMA default;

# COMMAND ----------

# MAGIC %md
# MAGIC **Notes:**
# MAGIC - The bundle YAML in this notebook (Examples 1 to 3, and the Manual setup required
# MAGIC   section) is illustrative and was not deployed via the Databricks CLI as part of
# MAGIC   writing this notebook; only the SQL DDL cells were designed to be run and verified
# MAGIC   directly in a workspace.
# MAGIC - Catalog and schema bundle resources require Databricks CLI 0.287.0 or later with the
# MAGIC   direct deployment engine.
# MAGIC - `CAN_MANAGE` (bundle YAML) and `MANAGE` (SQL `GRANT`) refer to the same underlying
# MAGIC   Unity Catalog privilege; the two surfaces just name it differently.
