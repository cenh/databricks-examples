# Databricks notebook source
# MAGIC %md
# MAGIC # Managing Unity Catalog External Locations with Declarative Automation Bundles
# MAGIC
# MAGIC **Article:** [Managing Unity Catalog External Locations with Declarative Automation Bundles](https://medium.com/@cralle/managing-unity-catalog-external-locations-with-declarative-automation-bundles-f754263b74eb?sk=16d20730327139e996f9f4884bee71c2)
# MAGIC
# MAGIC **Author:** Christian Hansen (https://medium.com/@cralle)
# MAGIC
# MAGIC Declaring Unity Catalog external locations directly in a Databricks bundle's YAML, instead of Terraform or the REST API.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Setup
# MAGIC
# MAGIC This notebook uses the `testing.default` catalog and schema for anything that is scoped
# MAGIC to a catalog/schema (tables, PySpark reads). External locations and storage credentials
# MAGIC themselves are metastore-level objects in Unity Catalog, so most commands below are not
# MAGIC prefixed with a catalog or schema.
# MAGIC
# MAGIC Sample company used throughout: **CH Enterprise**.

# COMMAND ----------

# MAGIC %sql
# MAGIC USE CATALOG testing;
# MAGIC USE SCHEMA default;

# COMMAND ----------

# MAGIC %md
# MAGIC ## What Is an External Location?
# MAGIC
# MAGIC An external location in Unity Catalog is a reference to a cloud storage path (an ADLS
# MAGIC container, an S3 bucket, or a GCS bucket) secured by a storage credential. It is the
# MAGIC foundation for accessing external data in a governed, auditable way.
# MAGIC
# MAGIC Until recently there were two main ways to manage external locations programmatically:
# MAGIC the Databricks REST API, or Terraform via the Databricks Terraform provider. As of
# MAGIC Databricks CLI version 0.289.0, there is a third option: declaring them directly inside a
# MAGIC Declarative Automation Bundle.

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Declaring an External Location in a Bundle
# MAGIC
# MAGIC An `external_locations` block goes under `resources` in the bundle's `databricks.yml`.
# MAGIC This is bundle configuration, not notebook code: it is deployed with the Databricks CLI
# MAGIC (`databricks bundle deploy`), not run in a notebook cell. It is shown here as YAML for
# MAGIC reference, exactly as it would appear in `databricks.yml`.
# MAGIC
# MAGIC ```yaml
# MAGIC resources:
# MAGIC   external_locations:
# MAGIC     ch_enterprise_data_lake:
# MAGIC       name: ch_enterprise_data_lake
# MAGIC       url: 'abfss://data@chenterprisestorage.dfs.core.windows.net/data-lake'
# MAGIC       credential_name: ch_enterprise_storage_credential
# MAGIC       comment: 'External location managed by Declarative Automation Bundles'
# MAGIC ```
# MAGIC
# MAGIC Three fields are required:
# MAGIC - `name` - the identifier for the external location in Unity Catalog
# MAGIC - `url` - the cloud storage path (ADLS, S3, or GCS)
# MAGIC - `credential_name` - the name of the storage credential that authorizes access

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Adding Grants Inline
# MAGIC
# MAGIC Grants can be defined directly alongside the resource, no separate Terraform resource
# MAGIC block and no separate API call:
# MAGIC
# MAGIC ```yaml
# MAGIC resources:
# MAGIC   external_locations:
# MAGIC     ch_enterprise_data_lake:
# MAGIC       name: ch_enterprise_data_lake
# MAGIC       url: 'abfss://data@chenterprisestorage.dfs.core.windows.net/data-lake'
# MAGIC       credential_name: ch_enterprise_storage_credential
# MAGIC       comment: 'External location managed by Declarative Automation Bundles'
# MAGIC       grants:
# MAGIC         - principal: someone@chenterprise.com
# MAGIC           privileges: [CREATE_EXTERNAL_TABLE, READ_FILES]
# MAGIC ```

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. A Production-Ready Example
# MAGIC
# MAGIC ```yaml
# MAGIC resources:
# MAGIC   external_locations:
# MAGIC     ch_enterprise_data_lake:
# MAGIC       name: ch_enterprise_data_lake
# MAGIC       url: 'abfss://prod@chenterprisestorage.dfs.core.windows.net/data-lake'
# MAGIC       credential_name: ch_enterprise_storage_credential
# MAGIC       comment: 'Production data lake managed via Declarative Automation Bundles'
# MAGIC       read_only: false
# MAGIC       fallback: false
# MAGIC       enable_file_events: true
# MAGIC       file_event_queue:
# MAGIC         managed_aqs: {}
# MAGIC       grants:
# MAGIC         - principal: data-engineers@chenterprise.com
# MAGIC           privileges: [CREATE_EXTERNAL_TABLE, READ_FILES, WRITE_FILES]
# MAGIC         - principal: analysts@chenterprise.com
# MAGIC           privileges: [READ_FILES]
# MAGIC ```
# MAGIC
# MAGIC Deployed from a terminal with:
# MAGIC
# MAGIC ```bash
# MAGIC databricks bundle deploy --target prod
# MAGIC ```
# MAGIC
# MAGIC Important: defining external locations in Declarative Automation Bundles is only
# MAGIC supported with the direct deployment engine. See "Manual setup required" below for how
# MAGIC to enable it.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Manual setup required
# MAGIC
# MAGIC Everything above this point is bundle YAML, and everything from here through the
# MAGIC placeholder `CREATE EXTERNAL LOCATION` statement further down depends on real cloud
# MAGIC resources. None of it can run end-to-end inside this notebook:
# MAGIC
# MAGIC - `databricks bundle deploy` is a Databricks CLI command. It reads a `databricks.yml`
# MAGIC   file from a local project directory and talks to the Databricks REST API from outside
# MAGIC   the workspace. There is no way to invoke it from a notebook cell.
# MAGIC - Creating a real storage credential requires a cloud identity (an AWS IAM role, an
# MAGIC   Azure managed identity or service principal, or a GCP service account) that already
# MAGIC   has permission to read/write the target storage location. A fresh workspace and a
# MAGIC   fresh cloud subscription do not have this by default; it has to be created once,
# MAGIC   outside of Databricks, by whoever administers the cloud account.
# MAGIC
# MAGIC ### Prerequisites
# MAGIC
# MAGIC 1. Databricks CLI version 0.289.0 or above (`external_locations` as a bundle resource
# MAGIC    was added at this version).
# MAGIC 2. A Unity Catalog metastore attached to the workspace, and `CREATE EXTERNAL LOCATION`
# MAGIC    privilege on that metastore (metastore admins have it by default).
# MAGIC 3. A cloud storage container/bucket (ADLS container, S3 bucket, or GCS bucket) and a
# MAGIC    cloud identity that can access it, created by your cloud/platform team. See
# MAGIC    [Create a storage credential for connecting to AWS S3](https://docs.databricks.com/aws/en/connect/unity-catalog/cloud-storage/storage-credentials-s3)
# MAGIC    (or the ADLS/GCS equivalent) for the exact IAM role/trust policy or managed identity
# MAGIC    steps for your cloud.
# MAGIC
# MAGIC ### Step 1: Check the CLI version and upgrade if needed
# MAGIC
# MAGIC ```bash
# MAGIC databricks --version
# MAGIC # Upgrade if below 0.289.0, e.g. on macOS/Linux with Homebrew:
# MAGIC brew upgrade databricks
# MAGIC ```
# MAGIC
# MAGIC ### Step 2: Create the storage credential (once, outside the bundle)
# MAGIC
# MAGIC The storage credential itself is usually created once by a platform team and then
# MAGIC referenced by name from any number of bundles. Either through the Catalog Explorer UI
# MAGIC (Catalog > External Data > Credentials > Create credential), or via the CLI once the
# MAGIC underlying cloud role/identity exists:
# MAGIC
# MAGIC ```bash
# MAGIC databricks storage-credentials create ch_enterprise_storage_credential \
# MAGIC   --json '{
# MAGIC     "name": "ch_enterprise_storage_credential",
# MAGIC     "comment": "Storage credential for the CH Enterprise data lake",
# MAGIC     "azure_managed_identity": {
# MAGIC       "access_connector_id": "<YOUR_ACCESS_CONNECTOR_RESOURCE_ID>"
# MAGIC     }
# MAGIC   }'
# MAGIC ```
# MAGIC
# MAGIC (Substitute `aws_iam_role` or `gcp_service_account_key` for the equivalent AWS/GCP
# MAGIC payload; see the docs link in Prerequisites for the exact JSON shape per cloud.)
# MAGIC
# MAGIC ### Step 3: Enable the direct deployment engine in the bundle
# MAGIC
# MAGIC Add this to `databricks.yml` (Databricks CLI 1.3.0+ bundles created via `databricks
# MAGIC bundle init` default to it already):
# MAGIC
# MAGIC ```yaml
# MAGIC bundle:
# MAGIC   name: ch-enterprise-data-platform
# MAGIC   engine: direct
# MAGIC ```
# MAGIC
# MAGIC Or set it per invocation without editing the file:
# MAGIC
# MAGIC ```bash
# MAGIC DATABRICKS_BUNDLE_ENGINE=direct databricks bundle deploy --target dev
# MAGIC ```
# MAGIC
# MAGIC ### Step 4: Add the `external_locations` resource and deploy
# MAGIC
# MAGIC Add the YAML block from section 1, 2, or 3 above to `resources` in `databricks.yml`,
# MAGIC referencing the storage credential created in Step 2 by name, then from a terminal in
# MAGIC the bundle's root directory:
# MAGIC
# MAGIC ```bash
# MAGIC databricks bundle validate --target dev
# MAGIC databricks bundle deploy --target dev
# MAGIC ```
# MAGIC
# MAGIC ### Step 5: Tear it down when done
# MAGIC
# MAGIC ```bash
# MAGIC databricks bundle destroy --target dev
# MAGIC ```
# MAGIC
# MAGIC Once the storage credential and external location exist (created manually as above),
# MAGIC the SQL cells further down in this notebook that reference `ch_enterprise_data_lake` and
# MAGIC `ch_enterprise_storage_credential` can run as-is instead of erroring on a missing
# MAGIC credential.

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Verifying Storage Credentials and External Locations (runnable)
# MAGIC
# MAGIC These are read-only introspection statements. They run in any Unity Catalog workspace,
# MAGIC whether or not any storage credentials or external locations have been created yet.

# COMMAND ----------

# MAGIC %sql
# MAGIC SHOW STORAGE CREDENTIALS;

# COMMAND ----------

# MAGIC %sql
# MAGIC SHOW EXTERNAL LOCATIONS;

# COMMAND ----------

# MAGIC %md
# MAGIC The same information is easy to filter or post-process with PySpark instead of plain SQL,
# MAGIC for example to check whether the CH Enterprise data lake location already exists:

# COMMAND ----------

import pyspark.sql.functions as F

external_locations_df = spark.sql("SHOW EXTERNAL LOCATIONS")

(
    external_locations_df
    .filter(F.col("name").like("ch_enterprise%"))
    .select("name", "url", "comment")
    .show(truncate=False)
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5. Creating an External Location via SQL (placeholder values)
# MAGIC
# MAGIC This mirrors what the bundle YAML in section 1 declares, but as a direct SQL statement.
# MAGIC It is included for completeness and will only succeed once the "Manual setup required"
# MAGIC steps above have been completed in your own workspace: a real storage credential named
# MAGIC `ch_enterprise_storage_credential` and a real, accessible cloud storage path must already
# MAGIC exist. `<...>` marks the values you must replace.

# COMMAND ----------

# MAGIC %md
# MAGIC Reference only (not executed): this needs a real storage credential and a real, accessible
# MAGIC cloud path, so it is shown as syntax rather than run. Replace `<YOUR_CLOUD_STORAGE_URL>` with
# MAGIC a real ADLS/S3/GCS path and ensure `ch_enterprise_storage_credential` already exists (see
# MAGIC "Manual setup required" above), then run it in your own workspace.
# MAGIC
# MAGIC ```sql
# MAGIC CREATE EXTERNAL LOCATION IF NOT EXISTS ch_enterprise_data_lake
# MAGIC   URL '<YOUR_CLOUD_STORAGE_URL>'
# MAGIC   WITH (STORAGE CREDENTIAL ch_enterprise_storage_credential)
# MAGIC   COMMENT 'External location for the CH Enterprise data lake, managed via Declarative Automation Bundles';
# MAGIC ```

# COMMAND ----------

# MAGIC %md
# MAGIC ## 6. Describing and Granting on the External Location
# MAGIC
# MAGIC These statements also depend on `ch_enterprise_data_lake` already existing from the
# MAGIC previous cell (or from a bundle deploy).

# COMMAND ----------

# MAGIC %md
# MAGIC Reference only (depends on `ch_enterprise_data_lake` existing from a real deploy):
# MAGIC
# MAGIC ```sql
# MAGIC DESCRIBE EXTERNAL LOCATION ch_enterprise_data_lake;
# MAGIC ```

# COMMAND ----------

# MAGIC %md
# MAGIC Reference only (inline bundle grants map to these SQL `GRANT` statements; they need the
# MAGIC external location to exist first):
# MAGIC
# MAGIC ```sql
# MAGIC GRANT CREATE EXTERNAL TABLE, READ FILES, WRITE FILES
# MAGIC   ON EXTERNAL LOCATION ch_enterprise_data_lake
# MAGIC   TO `data-engineers@chenterprise.com`;
# MAGIC
# MAGIC GRANT READ FILES
# MAGIC   ON EXTERNAL LOCATION ch_enterprise_data_lake
# MAGIC   TO `analysts@chenterprise.com`;
# MAGIC ```

# COMMAND ----------

# MAGIC %md
# MAGIC Reference only (depends on the external location existing):
# MAGIC
# MAGIC ```sql
# MAGIC SHOW GRANTS ON EXTERNAL LOCATION ch_enterprise_data_lake;
# MAGIC ```

# COMMAND ----------

# MAGIC %md
# MAGIC ## Bundles vs. Terraform: When to Use Which
# MAGIC
# MAGIC | Area | Declarative Automation Bundles | Terraform |
# MAGIC | --- | --- | --- |
# MAGIC | Co-location with pipelines | Same YAML, deploys together | Separate repo/project |
# MAGIC | CI/CD integration | Native, single `databricks bundle deploy` | Needs its own pipeline |
# MAGIC | Grants management | Inline, alongside the resource | Separate resource blocks |
# MAGIC | Lifecycle control | `lifecycle` block in the same YAML | Terraform lifecycle meta-arguments |
# MAGIC | State management | Stateless; the platform is the source of truth | Requires a remote state backend |
# MAGIC | Learning curve | Pure YAML | Requires HCL and Terraform concepts |
# MAGIC
# MAGIC Rule of thumb from the article: if the platform/infrastructure team owns the external
# MAGIC location and it changes rarely, Terraform is the natural fit. If the data engineering
# MAGIC team owns it and it evolves alongside pipelines and jobs, the bundle is the right home.
# MAGIC Terraform is still the better choice when the location spans multiple workspaces, is
# MAGIC shared across many teams' bundles, already lives in a mature Terraform/Unity Catalog
# MAGIC setup, or when you need Terraform's plan/apply diff and approval workflow.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Cleanup

# COMMAND ----------

# MAGIC %sql
# MAGIC DROP EXTERNAL LOCATION IF EXISTS ch_enterprise_data_lake;

# COMMAND ----------

# MAGIC %md
# MAGIC The storage credential created manually in "Manual setup required" (Step 2) is not
# MAGIC dropped by this notebook, since other objects may depend on it. Remove it yourself once
# MAGIC you are done experimenting:
# MAGIC
# MAGIC ```bash
# MAGIC databricks storage-credentials delete ch_enterprise_storage_credential
# MAGIC ```
# MAGIC
# MAGIC If you deployed the bundle from section 3, tear it down with:
# MAGIC
# MAGIC ```bash
# MAGIC databricks bundle destroy --target dev
# MAGIC ```

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC **Source:** [Declarative Automation Bundles resources reference](https://docs.databricks.com/aws/en/dev-tools/bundles/resources)
# MAGIC
# MAGIC **Notes:**
# MAGIC - Requires Databricks CLI 0.289.0 or above to declare `external_locations` as a bundle resource
# MAGIC - Only supported with the direct deployment engine (`bundle: engine: direct` in `databricks.yml`, CLI 0.279.0+)
# MAGIC - Three required fields on the resource: `name`, `url`, `credential_name`
# MAGIC - Inline `grants` remove the need for separate GRANT statements or Terraform grant resources
# MAGIC - File event queues are supported across AWS, Azure, and GCP
# MAGIC - `databricks bundle deploy` runs from a terminal against a local `databricks.yml`; it cannot be invoked from inside a notebook cell
# MAGIC - Creating the storage credential itself needs a real cloud identity (IAM role, managed identity, or service account) set up outside Databricks
# MAGIC - The `CREATE EXTERNAL LOCATION`, `DESCRIBE`, `GRANT`, and `SHOW GRANTS` cells are shown as reference only, since they require a real storage credential and cloud path; the `SHOW STORAGE CREDENTIALS` / `SHOW EXTERNAL LOCATIONS` introspection cells run in any Unity Catalog workspace
