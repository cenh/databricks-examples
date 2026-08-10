# Databricks notebook source
# MAGIC %md
# MAGIC # Stop Writing Row Filters Table by Table: ABAC Is Now GA in Unity Catalog
# MAGIC
# MAGIC **Article:** [Stop Writing Row Filters Table by Table: ABAC Is Now GA in Unity Catalog](https://medium.com/@cralle/govern-once-protect-everywhere-abac-row-filtering-and-column-masking-is-ga-in-unity-catalog-0cfc1165db70?sk=19d73edd3ac4c11f114cc53065ac8370)
# MAGIC
# MAGIC Companion notebook: a PySpark walkthrough of Attribute-Based Access Control (ABAC), Governed Tags, and Data Classification in Unity Catalog.
# MAGIC
# MAGIC This notebook walks through the full pattern end to end:
# MAGIC 1. Create governed tags
# MAGIC 2. Apply tags to catalogs, tables, and columns
# MAGIC 3. Register masking and row-filter UDFs in Unity Catalog
# MAGIC 4. Create catalog-level ABAC policies
# MAGIC 5. Verify the policies from PySpark
# MAGIC 6. Inspect limitations and clean up
# MAGIC
# MAGIC **Requirements**
# MAGIC - Unity Catalog enabled workspace
# MAGIC - Databricks Runtime 16.4+ or serverless compute (required to read ABAC-protected tables)
# MAGIC - `MANAGE` privilege on the target catalog
# MAGIC - Permission to `CREATE` governed tags (workspace admin by default, configurable)
# MAGIC
# MAGIC **Note on DDL**: ABAC and Governed Tag DDL is SQL-only at the time of writing, so the PySpark pattern is to drive everything through `spark.sql()`. The read path stays pure DataFrame.

# COMMAND ----------

# MAGIC %md
# MAGIC ## 0. Parameters
# MAGIC
# MAGIC Set the catalog and schema you want to use as the demo target. The notebook never touches anything outside of `CATALOG.SCHEMA`.

# COMMAND ----------

dbutils.widgets.text("catalog", "prod_analytics", "Target catalog")
dbutils.widgets.text("schema", "customer", "Target schema")
dbutils.widgets.text("security_schema", "security", "Schema for UDFs")

CATALOG = dbutils.widgets.get("catalog")
SCHEMA = dbutils.widgets.get("schema")
SECURITY_SCHEMA = dbutils.widgets.get("security_schema")

# Some Unity Catalog DDL (notably CREATE FUNCTION) requires the session's current
# catalog to match the target catalog, otherwise it raises:
#   "Fail to execute the command as the target schema is not in the current catalog.
#    Please set the current catalog with 'USE CATALOG <name>' first."
# Set it up front so every subsequent cell runs in the right context.
spark.sql(f"USE CATALOG {CATALOG}")

print(f"Catalog:        {CATALOG}")
print(f"Data schema:    {SCHEMA}")
print(f"Security schema: {SECURITY_SCHEMA}")
print(f"Current catalog: {spark.sql('SELECT current_catalog()').collect()[0][0]}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Define the governed tag taxonomy
# MAGIC
# MAGIC Governed tags are account-level. You normally only run this once, and only a governance admin runs it. Each governed tag carries a tag policy that enforces who can apply it and which values are permitted.
# MAGIC
# MAGIC `CREATE GOVERNED TAG` does not support `IF NOT EXISTS` or `OR REPLACE`, and governed tags persist at the account level, so re-running this cell raises `ALREADY_EXISTS`. The helper below swallows that specific error so the notebook stays re-runnable.

# COMMAND ----------

def create_governed_tag_if_absent(tag_key: str, ddl: str) -> None:
    """Run a CREATE GOVERNED TAG statement, tolerating an existing account-level tag."""
    try:
        spark.sql(ddl)
        print(f"Created governed tag: {tag_key}")
    except Exception as e:  # noqa: BLE001 - narrow check on the message below
        if "ALREADY_EXISTS" in str(e):
            print(f"Governed tag already exists, skipping: {tag_key}")
        else:
            raise

create_governed_tag_if_absent("sensitivity", """
CREATE GOVERNED TAG sensitivity
  DESCRIPTION 'Business sensitivity tier applied to columns and tables'
  VALUES ('public', 'internal', 'confidential', 'restricted')
""")

create_governed_tag_if_absent("data_domain", """
CREATE GOVERNED TAG data_domain
  DESCRIPTION 'Owning business domain for the data asset'
  VALUES ('finance', 'marketing', 'hr', 'product')
""")

# COMMAND ----------

# MAGIC %md
# MAGIC Inspect the tags you just created. `SHOW GOVERNED TAGS` lists every governed tag in the account; `DESCRIBE GOVERNED TAG` returns the allowed values and policy details for one tag.

# COMMAND ----------

display(spark.sql("SHOW GOVERNED TAGS"))

# COMMAND ----------

display(spark.sql("DESCRIBE GOVERNED TAG sensitivity"))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Prepare a demo table (optional)
# MAGIC
# MAGIC If you do not already have a target table, this cell creates a small `transactions` table you can tag and read against. Skip this section if you are pointing at real data.

# COMMAND ----------

spark.sql(f"CREATE SCHEMA IF NOT EXISTS {CATALOG}.{SCHEMA}")
spark.sql(f"CREATE SCHEMA IF NOT EXISTS {CATALOG}.{SECURITY_SCHEMA}")

spark.sql(f"""
CREATE OR REPLACE TABLE {CATALOG}.{SCHEMA}.transactions (
  customer_id BIGINT,
  email       STRING,
  region      STRING,
  amount      DECIMAL(12, 2)
)
""")

spark.sql(f"""
INSERT INTO {CATALOG}.{SCHEMA}.transactions VALUES
  (1, 'alice@example.com',  'finance',   125.40),
  (2, 'bob@example.com',    'marketing',  17.95),
  (3, 'carol@example.com',  'finance',   980.00),
  (4, 'dave@example.com',   'hr',         45.00),
  (5, 'eve@example.com',    'product',   312.10)
""")

display(spark.read.table(f"{CATALOG}.{SCHEMA}.transactions"))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Apply tags to objects
# MAGIC
# MAGIC Tags can be applied at the catalog, schema, table, or column level. Catalog and schema tags inherit downward to every child object (except individual columns, which must be tagged directly).
# MAGIC
# MAGIC Setting tags at the catalog level is the high-leverage move: every new schema and table in the catalog inherits them automatically.

# COMMAND ----------

def set_tag_if_absent(label: str, ddl: str) -> None:
    """Apply a SET TAG assignment, tolerating an assignment that already exists.

    SET TAG is not an upsert: re-applying a tag key that is already assigned
    raises ALREADY_EXISTS.UC_DUPLICATE_TAG_ASSIGNMENT_CREATION. Swallow that
    specific error so the notebook stays re-runnable.
    """
    try:
        spark.sql(ddl)
        print(f"Tag applied: {label}")
    except Exception as e:  # noqa: BLE001 - narrow check on the message below
        if "ALREADY_EXISTS" in str(e):
            print(f"Tag assignment already exists, skipping: {label}")
        else:
            raise

set_tag_if_absent(
    "catalog data_domain=finance",
    f"SET TAG ON CATALOG {CATALOG} data_domain = finance",
)

set_tag_if_absent(
    "transactions.email sensitivity=confidential",
    f"SET TAG ON COLUMN {CATALOG}.{SCHEMA}.transactions.email sensitivity = confidential",
)

set_tag_if_absent(
    "transactions.region data_domain=finance",
    f"SET TAG ON COLUMN {CATALOG}.{SCHEMA}.transactions.region data_domain = finance",
)

# COMMAND ----------

# MAGIC %md
# MAGIC If Data Classification is enabled on the catalog, system governed tags such as `class.email_address`, `class.phone_number`, or `class.us_ssn` will appear on these columns within roughly 24 hours of new data arriving, with no manual `ALTER` needed.

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Register masking and row-filter UDFs
# MAGIC
# MAGIC ABAC delegates the actual transformation to a Unity Catalog UDF. At GA, a single `VARIANT`-typed UDF can mask `INT`, `DOUBLE`, `DECIMAL`, and `STRUCT` columns together, which collapses a lot of policy sprawl. For string columns the pattern is still simple and explicit.
# MAGIC
# MAGIC We register the UDFs in a dedicated `security` schema so policy authors and table owners have a single place to find them.

# COMMAND ----------

spark.sql(f"""
CREATE OR REPLACE FUNCTION {CATALOG}.{SECURITY_SCHEMA}.mask_email(value STRING)
RETURNS STRING
RETURN CASE
  WHEN is_account_group_member('pii_readers') THEN value
  WHEN value IS NULL THEN NULL
  ELSE regexp_replace(value, '(^.).+(@.+$)', '$1***$2')
END
""")

spark.sql(f"""
CREATE OR REPLACE FUNCTION {CATALOG}.{SECURITY_SCHEMA}.filter_by_domain(domain STRING)
RETURNS BOOLEAN
RETURN is_account_group_member(CONCAT('domain_', domain))
   OR is_account_group_member('global_readers')
""")

# COMMAND ----------

# MAGIC %md
# MAGIC The first function partially masks an email unless the caller is in the `pii_readers` group. The second is a row-filter UDF that says "you can see this row if you belong to the matching domain group, or to a global reader group."
# MAGIC
# MAGIC Confirm both functions exist:

# COMMAND ----------

display(spark.sql(f"SHOW USER FUNCTIONS IN {CATALOG}.{SECURITY_SCHEMA}"))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5. Create the ABAC policies
# MAGIC
# MAGIC This is where attribute-driven access actually happens. Policies live on catalogs, schemas, or tables and reference tags through a `MATCH COLUMNS` clause that uses `has_tag(...)` or `has_tag_value(...)`.
# MAGIC
# MAGIC **Read these in plain English**
# MAGIC - Policy 1: across the whole `{CATALOG}` catalog, find any column whose tag matches `sensitivity = 'confidential'`, alias it as `target_col`, and apply `mask_email` to that column for every account user. (Group exemptions live inside the UDF.)
# MAGIC - Policy 2: across the same catalog, find any column with a `data_domain` tag, alias it as `region_col`, and pass its value into `filter_by_domain` as the row-filter input.
# MAGIC
# MAGIC **Two syntax details worth knowing**
# MAGIC - The condition functions are `has_tag('tag')` and `has_tag_value('tag', 'value')`, combinable with `AND`, `OR`, and `NOT`.
# MAGIC - The UDF after `ROW FILTER` and `COLUMN MASK` is referenced **without arguments**. Its inputs come from the alias you declared in `MATCH COLUMNS`, then either `ON COLUMN <alias>` (column mask) or `USING COLUMNS (<alias>)` (row filter).

# COMMAND ----------

# Column mask: any column tagged sensitivity=confidential gets masked
spark.sql(f"""
CREATE OR REPLACE POLICY mask_confidential_emails
ON CATALOG {CATALOG}
COMMENT 'Mask any column tagged sensitivity=confidential'
COLUMN MASK {CATALOG}.{SECURITY_SCHEMA}.mask_email
TO `account users`
FOR TABLES
MATCH COLUMNS has_tag_value('sensitivity', 'confidential') AS target_col
ON COLUMN target_col
""")

# Row filter: any table tagged with data_domain gets filtered by region
spark.sql(f"""
CREATE OR REPLACE POLICY domain_row_filter
ON CATALOG {CATALOG}
COMMENT 'Restrict rows by region for tables in a governed data domain'
ROW FILTER {CATALOG}.{SECURITY_SCHEMA}.filter_by_domain
TO `account users`
FOR TABLES
MATCH COLUMNS has_tag('data_domain') AS region_col
USING COLUMNS (region_col)
""")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 6. Verify from PySpark
# MAGIC
# MAGIC The whole point of ABAC is that the read path is unchanged. Analysts and pipelines query the table normally, and ABAC does its work transparently.
# MAGIC
# MAGIC - If the calling identity is **not** in `pii_readers`, the `email` column comes back as `a***@example.com`.
# MAGIC - If the identity is **not** in `domain_finance` or `global_readers`, the rows where `region = 'finance'` simply do not appear.
# MAGIC
# MAGIC Same DataFrame code, different result per user, zero per-table configuration.

# COMMAND ----------

df = (
    spark.read.table(f"{CATALOG}.{SCHEMA}.transactions")
    .select("customer_id", "email", "region", "amount")
)

df.show(5, truncate=False)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 7. Guardrails to keep in mind
# MAGIC
# MAGIC A few limitations worth remembering when designing your policy set:
# MAGIC
# MAGIC - You need DBR 16.4 or above (or serverless) to read ABAC-protected tables.
# MAGIC - Time travel, deep clones, and shallow clones do not work unless the user is exempt from the policy.
# MAGIC - Only one distinct row filter and one distinct column mask can resolve per query. If two policies produce different filters for the same user on the same table, the query is rejected, so design your conditions to be mutually exclusive.
# MAGIC - Views cannot have ABAC policies attached directly (tables, including streaming tables and materialized views, are the only supported target), but ABAC policies on the underlying tables are still enforced when you read through a view, evaluated against the identity of the querying user rather than the view owner.
# MAGIC - There is no information schema view for ABAC. `information_schema.row_filters` and `information_schema.column_masks` only show classic per-table filters and masks, not the ABAC-derived ones.
# MAGIC - Vector search indexes cannot be created from tables protected by ABAC row filters or column masks.

# COMMAND ----------

# MAGIC %md
# MAGIC ## 8. (Optional) Cleanup
# MAGIC
# MAGIC Run this section to undo the demo. Order matters: drop policies first, then UDFs, then tag assignments, then governed tags, and finally the demo table.

# COMMAND ----------

# Drop policies (DROP POLICY does not support IF EXISTS; comment out if not created)
spark.sql(f"DROP POLICY mask_confidential_emails ON CATALOG {CATALOG}")
spark.sql(f"DROP POLICY domain_row_filter ON CATALOG {CATALOG}")

# Drop UDFs
spark.sql(f"DROP FUNCTION IF EXISTS {CATALOG}.{SECURITY_SCHEMA}.mask_email")
spark.sql(f"DROP FUNCTION IF EXISTS {CATALOG}.{SECURITY_SCHEMA}.filter_by_domain")

# Remove tag assignments
spark.sql(f"UNSET TAG ON COLUMN {CATALOG}.{SCHEMA}.transactions.email sensitivity")
spark.sql(f"UNSET TAG ON COLUMN {CATALOG}.{SCHEMA}.transactions.region data_domain")
spark.sql(f"UNSET TAG ON CATALOG {CATALOG} data_domain")

# Drop governed tags (account-level; only do this if you really want to retire the taxonomy).
# DROP GOVERNED TAG does not support IF EXISTS.
# spark.sql("DROP GOVERNED TAG sensitivity")
# spark.sql("DROP GOVERNED TAG data_domain")

# Drop the demo table
spark.sql(f"DROP TABLE IF EXISTS {CATALOG}.{SCHEMA}.transactions")

print("Cleanup complete.")
