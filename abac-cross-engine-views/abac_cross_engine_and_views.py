# Databricks notebook source
# MAGIC %md
# MAGIC # Databricks ABAC: External Engines (GA) and Views (Beta)
# MAGIC
# MAGIC Companion notebook for the Medium article *Databricks ABAC Now Follows Your Data: External Engines and Views*.
# MAGIC Every object is created in `testing.default`. Change the catalog and schema in the cells if you use different names.
# MAGIC Sample company: **CH Enterprise**.
# MAGIC
# MAGIC **Prerequisites**
# MAGIC - Governed tags `pii` (allowed value `email`) and `geo` (allowed value `region`) exist at the account level, and you have `ASSIGN` on both. Tagging also needs `APPLY TAG` on the table and view, which you have as their owner.
# MAGIC - Account groups `ch_governance` and `ch_eu_partners` exist. Add yourself to `ch_governance` if you want to see unmasked values.
# MAGIC - Part 1: a metastore admin has turned on **External data access** (Catalog Explorer, gear icon, Metastore, Details tab).
# MAGIC - Part 2: a workspace admin has enabled the **ABAC on Views** preview, and this notebook runs on Databricks Runtime 19 or above (or serverless).
# MAGIC - You have `MANAGE` on the schema (to create policies) and are allowed to create functions in it.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Setup: sample CH Enterprise tables

# COMMAND ----------

# MAGIC %sql
# MAGIC USE CATALOG testing;
# MAGIC USE SCHEMA default;

# COMMAND ----------

# MAGIC %md
# MAGIC ### Step 1: managed table with catalog commits
# MAGIC Every job that reads or writes a table with catalog commits needs Databricks Runtime 16.4 or above.

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Managed Delta table with catalog commits (required for cross-engine ABAC, Databricks Runtime 16.4+)
# MAGIC CREATE TABLE IF NOT EXISTS testing.default.ch_customers (
# MAGIC   customer_id INT,
# MAGIC   name STRING,
# MAGIC   email STRING,
# MAGIC   region STRING,
# MAGIC   lifetime_value DECIMAL(10, 2)
# MAGIC )
# MAGIC USING delta
# MAGIC TBLPROPERTIES ('delta.feature.catalogManaged' = 'supported');

# COMMAND ----------

import pyspark.sql.functions as F
from decimal import Decimal

customers = spark.createDataFrame(
    [
        (1, "Dana Lund", "dana.lund@chenterprise.com", "EU", Decimal("1250.00")),
        (2, "Omar Haddad", "omar.haddad@chenterprise.com", "US", Decimal("980.50")),
        (3, "Mei Tanaka", "mei.tanaka@chenterprise.com", "APAC", Decimal("4310.75")),
        (4, "Lars Berg", "lars.berg@chenterprise.com", "EU", Decimal("615.20")),
    ],
    "customer_id INT, name STRING, email STRING, region STRING, lifetime_value DECIMAL(10, 2)",
)
# insertInto keeps the table definition (and its catalogManaged feature) instead of replacing the table
customers.write.insertInto("testing.default.ch_customers", overwrite=True)

orders = spark.createDataFrame(
    [
        (101, "dana.lund@chenterprise.com", "EU", Decimal("120.00")),
        (102, "omar.haddad@chenterprise.com", "US", Decimal("75.40")),
        (103, "mei.tanaka@chenterprise.com", "APAC", Decimal("310.00")),
        (104, "lars.berg@chenterprise.com", "EU", Decimal("42.90")),
    ],
    "order_id INT, customer_email STRING, region STRING, amount DECIMAL(10, 2)",
)
# ch_orders plays the role of a table owned by another team: it is deliberately left untagged
orders.write.mode("overwrite").saveAsTable("testing.default.ch_orders")

display(spark.table("testing.default.ch_orders").orderBy(F.col("order_id")))

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Confirm catalog commits are enabled: look for catalogManaged in tableFeatures
# MAGIC DESCRIBE DETAIL testing.default.ch_customers;

# COMMAND ----------

# MAGIC %md
# MAGIC ## Part 1: Cross-engine ABAC (GA)
# MAGIC ### Step 2: tag the column, create the mask, attach a schema-level policy

# COMMAND ----------

# MAGIC %sql
# MAGIC SET TAG ON COLUMN testing.default.ch_customers.email pii = email;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Returns STRING for a STRING column (external engines require the original type)
# MAGIC -- and is idempotent: masking an already-masked value returns the same value
# MAGIC CREATE OR REPLACE FUNCTION testing.default.ch_mask_email(email STRING)
# MAGIC   RETURNS STRING
# MAGIC   RETURN CASE
# MAGIC     WHEN email NOT LIKE '%@%' THEN '***'
# MAGIC     ELSE concat('***@', substring_index(email, '@', -1))
# MAGIC   END;

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE POLICY ch_pii_email
# MAGIC ON SCHEMA testing.default
# MAGIC COMMENT 'Mask customer emails for everyone outside governance'
# MAGIC COLUMN MASK testing.default.ch_mask_email
# MAGIC TO `account users`
# MAGIC EXCEPT `ch_governance`
# MAGIC FOR TABLES
# MAGIC MATCH COLUMNS has_tag_value('pii', 'email') AS email
# MAGIC ON COLUMN email;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Tag changes can take a few minutes to take effect.
# MAGIC -- Unless you are in ch_governance, every email shows as ***@chenterprise.com
# MAGIC SELECT * FROM testing.default.ch_customers ORDER BY customer_id;

# COMMAND ----------

# MAGIC %md
# MAGIC ### Step 3: grant the external service principal
# MAGIC Replace the placeholder with the service principal's application ID. Only the parent catalog owner can grant `EXTERNAL USE SCHEMA`.

# COMMAND ----------

# MAGIC %sql
# MAGIC -- GRANT USE CATALOG ON CATALOG testing TO `<ch-external-spark-app-id>`;
# MAGIC -- GRANT USE SCHEMA, SELECT ON SCHEMA testing.default TO `<ch-external-spark-app-id>`;
# MAGIC -- GRANT EXTERNAL USE SCHEMA ON SCHEMA testing.default TO `<ch-external-spark-app-id>`;
# MAGIC SELECT 'Uncomment the grants above and fill in the service principal application ID' AS note;

# COMMAND ----------

# MAGIC %md
# MAGIC ### Step 4: read from external Apache Spark (runs outside Databricks)
# MAGIC This cell is shown for reference and is meant for your own Spark 4 cluster with Delta-Spark 4.1+ and the Unity Catalog Spark connector 0.4+.
# MAGIC `ServerSidePlanning.enabled = true` hands scan planning to Unity Catalog, which is what turns on enforcement for external reads.
# MAGIC `hadoop-aws` and `fs.s3.impl` are the AWS variant; on Azure use `org.apache.hadoop:hadoop-azure:3.4.2` and no `fs` line.
# MAGIC For a workspace service principal, `<oauth-token-endpoint>` is `<workspace-url>/oidc/v1/token`.
# MAGIC
# MAGIC ```python
# MAGIC from pyspark.sql import SparkSession
# MAGIC
# MAGIC spark = (
# MAGIC     SparkSession.builder.appName("ch-external-reader")
# MAGIC     .config(
# MAGIC         "spark.jars.packages",
# MAGIC         "io.delta:delta-spark_4.0_2.13:4.1.0,"
# MAGIC         "io.delta:delta-iceberg_2.13:4.1.0,"
# MAGIC         "io.unitycatalog:unitycatalog-spark_2.13:0.4.0,"
# MAGIC         "org.apache.hadoop:hadoop-aws:3.4.1",
# MAGIC     )
# MAGIC     .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
# MAGIC     .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
# MAGIC     .config("spark.sql.catalog.testing", "io.unitycatalog.spark.UCSingleCatalog")
# MAGIC     .config("spark.sql.catalog.testing.uri", "<workspace-url>")
# MAGIC     .config("spark.sql.catalog.testing.auth.type", "oauth")
# MAGIC     .config("spark.sql.catalog.testing.auth.oauth.uri", "<oauth-token-endpoint>")
# MAGIC     .config("spark.sql.catalog.testing.auth.oauth.clientId", "<oauth-client-id>")
# MAGIC     .config("spark.sql.catalog.testing.auth.oauth.clientSecret", "<oauth-client-secret>")
# MAGIC     .config("spark.sql.catalog.testing.ServerSidePlanning.enabled", "true")
# MAGIC     .config("spark.sql.defaultCatalog", "testing")
# MAGIC     .config("spark.hadoop.fs.s3.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
# MAGIC     .getOrCreate()
# MAGIC )
# MAGIC
# MAGIC spark.table("testing.default.ch_customers").show()
# MAGIC ```

# COMMAND ----------

# MAGIC %md
# MAGIC ### What it costs: serverless enforcement usage

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC   usage_date,
# MAGIC   sku_name,
# MAGIC   identity_metadata.run_as AS run_as,
# MAGIC   SUM(usage_quantity) AS dbus
# MAGIC FROM system.billing.usage
# MAGIC WHERE billing_origin_product = 'EXTERNAL_COMPATIBILITY'
# MAGIC   AND usage_date >= current_date() - INTERVAL 30 DAYS
# MAGIC GROUP BY ALL
# MAGIC ORDER BY usage_date;

# COMMAND ----------

# MAGIC %md
# MAGIC ## Part 2: ABAC on views (Beta)
# MAGIC Requires the **ABAC on Views** preview and Databricks Runtime 19+.
# MAGIC Without the preview, tagging the view's columns still succeeds, but `CREATE POLICY ... ON TABLE <view>` fails with
# MAGIC `UC_ABAC_UNSUPPORTED_TABLE_TYPE` ("ABAC policies are not supported for table type VIEW").

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Use ALTER VIEW ... AS to change this view later: CREATE OR REPLACE clears its tags and policies
# MAGIC CREATE VIEW IF NOT EXISTS testing.default.ch_orders_v AS
# MAGIC SELECT order_id, customer_email, region, amount
# MAGIC FROM testing.default.ch_orders;

# COMMAND ----------

# MAGIC %sql
# MAGIC SET TAG ON COLUMN testing.default.ch_orders_v.customer_email pii = email;

# COMMAND ----------

# MAGIC %sql
# MAGIC SET TAG ON COLUMN testing.default.ch_orders_v.region geo = region;

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE FUNCTION testing.default.ch_is_eu(region STRING)
# MAGIC   RETURNS BOOLEAN
# MAGIC   RETURN region = 'EU';

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE POLICY ch_eu_partners_only
# MAGIC ON TABLE testing.default.ch_orders_v
# MAGIC ROW FILTER testing.default.ch_is_eu
# MAGIC TO `ch_eu_partners`
# MAGIC FOR TABLES
# MAGIC MATCH COLUMNS has_tag_value('geo', 'region') AS region
# MAGIC USING COLUMNS (region);

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Expect ch_pii_email (inherited from the schema) and ch_eu_partners_only (attached to the view)
# MAGIC SHOW EFFECTIVE POLICIES ON TABLE testing.default.ch_orders_v;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- As a member of neither group: all regions, masked emails.
# MAGIC -- As a member of ch_eu_partners: only EU rows, masked emails.
# MAGIC -- As a member of ch_governance only: all regions, raw emails.
# MAGIC -- Group membership changes can take a few minutes to show up here.
# MAGIC SELECT
# MAGIC   current_user() AS querying_user,
# MAGIC   is_account_group_member('ch_governance') AS in_governance,
# MAGIC   is_account_group_member('ch_eu_partners') AS in_eu_partners,
# MAGIC   *
# MAGIC FROM testing.default.ch_orders_v
# MAGIC ORDER BY order_id;

# COMMAND ----------

# MAGIC %md
# MAGIC ## Cleanup (optional)

# COMMAND ----------

# MAGIC %sql
# MAGIC -- DROP POLICY ch_eu_partners_only ON TABLE testing.default.ch_orders_v;
# MAGIC -- DROP POLICY ch_pii_email ON SCHEMA testing.default;
# MAGIC -- DROP VIEW IF EXISTS testing.default.ch_orders_v;
# MAGIC -- DROP TABLE IF EXISTS testing.default.ch_orders;
# MAGIC -- DROP TABLE IF EXISTS testing.default.ch_customers;
# MAGIC SELECT 'Uncomment to clean up' AS note;

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC **Notes**
# MAGIC - Cross-engine ABAC is GA (October 1, 2026); external reads are read-only while policies are enforced, dynamic views are not supported, `VARIANT` projection and `BINARY` filtering are not supported, and masks must return the column's original type.
# MAGIC - ABAC on views is Beta (September 29, 2026); metric views are not supported, and at most 20 policies can apply to one query through a view hierarchy.
# MAGIC
# MAGIC **Sources**
# MAGIC - https://docs.databricks.com/aws/en/external-access/cross-engine-abac
# MAGIC - https://docs.databricks.com/aws/en/data-governance/unity-catalog/abac/requirements
# MAGIC - https://docs.databricks.com/aws/en/data-governance/unity-catalog/abac/policies
# MAGIC - https://docs.databricks.com/aws/en/release-notes/product/2026/october
# MAGIC - https://docs.databricks.com/aws/en/release-notes/product/2026/september
# MAGIC - https://docs.databricks.com/aws/en/tables/features/catalog-commits
# MAGIC - https://docs.databricks.com/aws/en/external-access/credential-vending
