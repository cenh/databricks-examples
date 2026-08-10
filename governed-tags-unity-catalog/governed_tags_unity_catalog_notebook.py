# Databricks notebook source
# MAGIC %md
# MAGIC # Mastering Governed Tags in Unity Catalog: Consistency, Compliance, and Control
# MAGIC
# MAGIC **Article:** [Mastering Governed Tags in Unity Catalog: Consistency, Compliance, and Control](https://medium.com/@cralle/mastering-governed-tags-in-unity-catalog-consistency-compliance-and-control-0bd85a8599bd?sk=5cf4ab7cedf3766e04d96db571305634)
# MAGIC
# MAGIC Author: Christian Hansen (https://medium.com/@cralle)
# MAGIC
# MAGIC Published: October 5, 2025
# MAGIC
# MAGIC Governed Tags bring account-level tag policies, allowed values, and assignment
# MAGIC permissions to Unity Catalog, so a tag like "costcenter" or "sensitivity" means the
# MAGIC same thing everywhere instead of drifting into "eng", "engineering", and "ENG_dept".

# COMMAND ----------

# MAGIC %md
# MAGIC ## Overview
# MAGIC
# MAGIC Governed Tags are account-level tags in Unity Catalog with an attached policy that
# MAGIC defines:
# MAGIC
# MAGIC - **Allowed values** - a single source of truth, so "engineering" cannot become "eng"
# MAGIC   or "ENG_dept".
# MAGIC - **Who can assign, create, or manage the tag** - through three permissions:
# MAGIC   `CREATE`, `MANAGE`, and `ASSIGN`, granted account-wide or per-tag.
# MAGIC - **Where the tag applies** - catalogs, schemas, tables, and columns.
# MAGIC
# MAGIC They matter for CH Enterprise because they turn tagging from an honor system into a
# MAGIC controlled part of the governance model:
# MAGIC
# MAGIC 1. **Data discovery** - consistent tags mean complete search results ("marketing", not
# MAGIC    a mix of "marketing" and "mktg").
# MAGIC 2. **Governance and compliance** - mark PII or financial data reliably, and combine tags
# MAGIC    with Attribute-Based Access Control (ABAC) for dynamic access decisions.
# MAGIC 3. **Cost management and reporting** - a `costcenter` tag that always resolves to the
# MAGIC    same values makes chargeback and cost attribution trustworthy.
# MAGIC 4. **Operational efficiency** - standardized metadata makes automation, monitoring, and
# MAGIC    lifecycle policies easier to build on top of.
# MAGIC
# MAGIC Governed Tags were in Public Preview at the time of writing. Creating the tag policy
# MAGIC itself and granting account-level permissions are account console operations and cannot
# MAGIC be scripted from a notebook; that part is called out explicitly below. Applying an
# MAGIC already-approved tag to a catalog, schema, table, or column, and reading tags back for
# MAGIC auditing, is regular SQL and is fully runnable here.
# MAGIC
# MAGIC This notebook:
# MAGIC
# MAGIC 1. Creates a small CH Enterprise catalog/schema with sample tables (**Setup**).
# MAGIC 2. Walks through the account console steps to define governed tag policies and
# MAGIC    permissions (**Manual Setup Required**).
# MAGIC 3. Applies tags to catalog, schema, table, and column objects.
# MAGIC 4. Shows the non-breaking migration path for tags that existed before governance.
# MAGIC 5. Reads tags back from `information_schema` and runs simple audit / adoption queries.
# MAGIC 6. Removes tag assignments and cleans up the sample objects (**Cleanup**).

# COMMAND ----------

# MAGIC %md
# MAGIC ## Setup
# MAGIC
# MAGIC Create the sandbox catalog and schema (`testing.default`) and three sample tables
# MAGIC that stand in for CH Enterprise data: customer orders, employee expenses, and marketing
# MAGIC campaign spend. These are the objects the rest of the notebook tags and audits.

# COMMAND ----------

catalog = "testing"
schema = "default"

spark.sql(f"CREATE CATALOG IF NOT EXISTS {catalog}")
spark.sql(f"CREATE SCHEMA IF NOT EXISTS {catalog}.{schema}")
spark.sql(f"USE CATALOG {catalog}")
spark.sql(f"USE SCHEMA {schema}")

# COMMAND ----------

import pyspark.sql.functions as F

customer_orders_data = [
    (1001, "Aiko Tanaka", "aiko.tanaka@chenterprise.com", 482.50, "APAC", "2026-01-14"),
    (1002, "Lars Berg", "lars.berg@chenterprise.com", 1290.00, "EMEA", "2026-02-03"),
    (1003, "Maria Gomez", "maria.gomez@chenterprise.com", 76.20, "AMER", "2026-02-19"),
    (1004, "Noah Fischer", "noah.fischer@chenterprise.com", 940.75, "EMEA", "2026-03-05"),
]
customer_orders_columns = [
    "order_id", "customer_name", "customer_email", "order_amount_usd", "region", "order_date",
]

customer_orders_df = (
    spark.createDataFrame(customer_orders_data, customer_orders_columns)
    .withColumn("order_date", F.to_date("order_date"))
    .withColumn("company", F.lit("CH Enterprise"))
)
customer_orders_df.write.mode("overwrite").saveAsTable(f"{catalog}.{schema}.customer_orders")

employee_expenses_data = [
    (2001, "Priya Shah", "priya.shah@chenterprise.com", "Engineering", 128.40, "2026-01-22"),
    (2002, "Tomas Novak", "tomas.novak@chenterprise.com", "Finance", 54.10, "2026-02-08"),
    (2003, "Elin Karlsson", "elin.karlsson@chenterprise.com", "Engineering", 312.90, "2026-02-27"),
]
employee_expenses_columns = [
    "expense_id", "employee_name", "employee_email", "department", "amount_usd", "expense_date",
]

employee_expenses_df = (
    spark.createDataFrame(employee_expenses_data, employee_expenses_columns)
    .withColumn("expense_date", F.to_date("expense_date"))
    .withColumn("amount_usd", F.round(F.col("amount_usd"), 2))
)
employee_expenses_df.write.mode("overwrite").saveAsTable(f"{catalog}.{schema}.employee_expenses")

marketing_campaigns_data = [
    (3001, "Spring Launch", "paid_social", 18500.00, "2026-01"),
    (3002, "Partner Webinar Series", "email", 4200.00, "2026-02"),
    (3003, "Trade Show EMEA", "events", 26750.00, "2026-03"),
]
marketing_campaigns_columns = [
    "campaign_id", "campaign_name", "channel", "spend_usd", "campaign_month",
]

marketing_campaigns_df = spark.createDataFrame(marketing_campaigns_data, marketing_campaigns_columns)
marketing_campaigns_df.write.mode("overwrite").saveAsTable(f"{catalog}.{schema}.marketing_campaigns")

print("Sample tables created:")
for table in ["customer_orders", "employee_expenses", "marketing_campaigns"]:
    print(f"  {catalog}.{schema}.{table}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Manual Setup Required (Account Console)
# MAGIC
# MAGIC Governed tag **policies** (the tag key, its allowed values, and who may
# MAGIC create/manage/assign it) are defined once per account, not per workspace, and this can
# MAGIC only be done through the account console or Catalog Explorer UI, not from a notebook.
# MAGIC Do this before running the tagging cells below if you want the allowed-value
# MAGIC validation to actually be enforced; the `ALTER ... SET TAGS` statements later in this
# MAGIC notebook will still run without it, they just will not be governed yet.
# MAGIC
# MAGIC ### 1. Create each governed tag
# MAGIC
# MAGIC For each tag key used in this notebook (`sensitivity`, `costcenter`, `owner`, `team`):
# MAGIC
# MAGIC 1. In the workspace sidebar, open **Catalog** (the Data icon).
# MAGIC 2. Click **Governed tags** in the left panel (account admins can also reach this from
# MAGIC    the account console under **Governance**).
# MAGIC 3. Click **Create governed tag**.
# MAGIC 4. Enter the **tag key** exactly as it will be used in SQL, for example `sensitivity`.
# MAGIC    Tag keys are case sensitive.
# MAGIC 5. Optionally add a **description** explaining what the key means and when to use it.
# MAGIC 6. Optionally click **Add allowed value** and enter each approved value with an optional
# MAGIC    description, for example:
# MAGIC    - `sensitivity`: `public`, `internal`, `confidential`, `restricted`
# MAGIC    - `costcenter`: `engineering`, `finance`, `marketing`, `sales`
# MAGIC    - `team`: `marketing`, `engineering`, `finance`, `sales`
# MAGIC    - `owner`: leave without allowed values if it should stay free text (a steward email)
# MAGIC 7. Click **Create**.
# MAGIC
# MAGIC ### 2. Grant permissions to create, manage, and assign tags
# MAGIC
# MAGIC There are three distinct permissions: `CREATE` (define new governed tags), `MANAGE`
# MAGIC (edit allowed values/descriptions, delete the tag), and `ASSIGN` (apply the tag to
# MAGIC objects). Grant them at whichever scope fits:
# MAGIC
# MAGIC - **Account level** (applies to all governed tags): **Governed tags** ->
# MAGIC   **Account Permissions** tab -> **Grant permission set** -> select the principals
# MAGIC   (users or groups, for example a `data-stewards` group) -> check `CREATE`, `MANAGE`,
# MAGIC   and/or `ASSIGN` as appropriate -> **Save**.
# MAGIC - **Per tag** (applies only to that one tag): select the tag from the **Governed tags**
# MAGIC   list -> **Permissions** tab -> **Grant permission set** -> select principals and
# MAGIC   permissions -> **Save**.
# MAGIC
# MAGIC Permission changes can take up to roughly 30 seconds (occasionally longer) to propagate
# MAGIC before they take effect for the affected principals.
# MAGIC
# MAGIC ### 3. Confirm the rollout
# MAGIC
# MAGIC Have a user with `ASSIGN` permission open a catalog, schema, table, or column in Catalog
# MAGIC Explorer and confirm the tag key now appears with a dropdown of allowed values in the
# MAGIC tag panel, rather than a free-text field.
# MAGIC
# MAGIC Everything from this point on (applying tags, migrating pre-existing tags, and reading
# MAGIC tags back for auditing) is expressed in SQL and runs directly in this notebook.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Applying Governed Tags to Objects
# MAGIC
# MAGIC Tags are applied with `ALTER ... SET TAGS`, the same syntax whether the key is governed
# MAGIC or not. If the key matches an existing governed tag policy, Unity Catalog validates the
# MAGIC value against the allowed list and checks the caller's `ASSIGN` permission; if not, it is
# MAGIC just a regular object tag. That is what lets the statements below run today even before
# MAGIC the manual policy setup above has been completed.

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Catalog-level tag: who owns this catalog
# MAGIC ALTER CATALOG testing SET TAGS ('owner' = 'data-platform-team@chenterprise.com');
# MAGIC
# MAGIC -- Schema-level tag: which cost center the schema's compute rolls up to by default
# MAGIC ALTER SCHEMA testing.default SET TAGS ('costcenter' = 'engineering');
# MAGIC
# MAGIC -- Table-level tags: sensitivity classification and the owning cost center
# MAGIC ALTER TABLE testing.default.customer_orders
# MAGIC   SET TAGS ('sensitivity' = 'confidential', 'costcenter' = 'sales');
# MAGIC
# MAGIC ALTER TABLE testing.default.employee_expenses
# MAGIC   SET TAGS ('sensitivity' = 'confidential', 'costcenter' = 'finance');
# MAGIC
# MAGIC ALTER TABLE testing.default.marketing_campaigns
# MAGIC   SET TAGS ('sensitivity' = 'internal', 'costcenter' = 'marketing');
# MAGIC
# MAGIC -- Column-level tags: flag the specific columns that hold PII with the most
# MAGIC -- restrictive sensitivity tier
# MAGIC ALTER TABLE testing.default.customer_orders
# MAGIC   ALTER COLUMN customer_email SET TAGS ('sensitivity' = 'restricted');
# MAGIC
# MAGIC ALTER TABLE testing.default.employee_expenses
# MAGIC   ALTER COLUMN employee_email SET TAGS ('sensitivity' = 'restricted');

# COMMAND ----------

# MAGIC %md
# MAGIC If a governed tag policy for `sensitivity` with a restricted allowed-value list has been
# MAGIC created (see Manual Setup Required above), assigning a value outside that list, for
# MAGIC example `'sensitivity' = 'top-secret'`, would fail with a permission/validation error
# MAGIC instead of silently applying. That behavior depends on account-console configuration, so
# MAGIC it is not executed here, only described.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Working with Existing (Non-Governed) Tags
# MAGIC
# MAGIC Turning a plain key into a governed tag is a non-breaking migration: existing
# MAGIC assignments with that key are not removed, and their current values simply become
# MAGIC linked to the new policy. Only new assignments have to use an allowed value going
# MAGIC forward. The cell below simulates that timeline for a `team` tag on the marketing
# MAGIC campaigns table: first the inconsistent, pre-governance value, then the corrected value
# MAGIC once the governed policy exists.

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Before governance: someone tagged the table with a shorthand value
# MAGIC ALTER TABLE testing.default.marketing_campaigns SET TAGS ('team' = 'mktg');
# MAGIC
# MAGIC -- After the "team" governed tag policy is created and rolled out (manual step above),
# MAGIC -- the old value "mktg" is left in place, but new assignments should use the approved
# MAGIC -- value. Re-tagging updates it to the standardized form:
# MAGIC ALTER TABLE testing.default.marketing_campaigns SET TAGS ('team' = 'marketing');

# COMMAND ----------

# MAGIC %md
# MAGIC ## Inspecting Applied Tags
# MAGIC
# MAGIC Tags on catalogs, schemas, tables, and columns can be read back from the `tags` views in
# MAGIC each catalog's `information_schema`. This is how you inventory what is already tagged
# MAGIC before rolling out a governed tag policy.

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT catalog_name, tag_name, tag_value
# MAGIC FROM testing.information_schema.catalog_tags;

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT catalog_name, schema_name, tag_name, tag_value
# MAGIC FROM testing.information_schema.schema_tags
# MAGIC WHERE schema_name = 'default';

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT catalog_name, schema_name, table_name, tag_name, tag_value
# MAGIC FROM testing.information_schema.table_tags
# MAGIC WHERE schema_name = 'default'
# MAGIC ORDER BY table_name, tag_name;

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT catalog_name, schema_name, table_name, column_name, tag_name, tag_value
# MAGIC FROM testing.information_schema.column_tags
# MAGIC WHERE schema_name = 'default'
# MAGIC ORDER BY table_name, column_name;

# COMMAND ----------

# MAGIC %md
# MAGIC ## Auditing Tag Consistency and Adoption
# MAGIC
# MAGIC Two of the best practices from the article, "Audit Regularly" and "Monitor Adoption
# MAGIC Metrics," come down to simple queries over the same `information_schema` views used
# MAGIC above: look for naming drift on a given key, and measure what share of objects actually
# MAGIC carry a required tag.

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Naming drift: the same tag key with multiple distinct values across objects is a sign
# MAGIC -- that key is a good candidate for (or is not yet fully enforcing) a governed tag policy.
# MAGIC SELECT tag_name, tag_value, COUNT(*) AS objects_tagged
# MAGIC FROM testing.information_schema.table_tags
# MAGIC GROUP BY tag_name, tag_value
# MAGIC ORDER BY tag_name, objects_tagged DESC;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Adoption: what share of tables in the schema carry the required "sensitivity" tag
# MAGIC SELECT
# MAGIC   COUNT(DISTINCT t.table_name) AS tables_in_schema,
# MAGIC   COUNT(DISTINCT tt.table_name) AS tables_with_sensitivity_tag,
# MAGIC   ROUND(COUNT(DISTINCT tt.table_name) * 100.0 / COUNT(DISTINCT t.table_name), 1) AS pct_tagged
# MAGIC FROM testing.information_schema.tables t
# MAGIC LEFT JOIN testing.information_schema.table_tags tt
# MAGIC   ON t.table_catalog = tt.catalog_name
# MAGIC   AND t.table_schema = tt.schema_name
# MAGIC   AND t.table_name = tt.table_name
# MAGIC   AND tt.tag_name = 'sensitivity'
# MAGIC WHERE t.table_catalog = 'testing' AND t.table_schema = 'default';

# COMMAND ----------

# MAGIC %md
# MAGIC ## Automating Backfill of Drifted Tags
# MAGIC
# MAGIC The "Automate Where Possible" best practice means writing small scripts that find and
# MAGIC correct legacy tag values rather than fixing them by hand, one object at a time. The cell
# MAGIC below finds every table still carrying the old `'team' = 'mktg'` shorthand and re-tags it
# MAGIC with the standardized value; it is a no-op here because the earlier cell already migrated
# MAGIC `marketing_campaigns`, but it is written to scale to any number of drifted tables.

# COMMAND ----------

drifted_rows = spark.sql(
    """
    SELECT table_name
    FROM testing.information_schema.table_tags
    WHERE tag_name = 'team' AND tag_value = 'mktg'
    """
).collect()

for row in drifted_rows:
    spark.sql(
        f"ALTER TABLE testing.default.{row.table_name} SET TAGS ('team' = 'marketing')"
    )

print(f"Re-tagged {len(drifted_rows)} table(s) from 'team' = 'mktg' to 'team' = 'marketing'.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## System Governed Tags
# MAGIC
# MAGIC Databricks also ships predefined **system governed tags**, shown with a wrench icon in
# MAGIC Catalog Explorer. They are not editable: their allowed values and assignment rules are
# MAGIC fixed by Databricks rather than by account admins. There is nothing to script here; they
# MAGIC already exist for every account and simply show up in the same `information_schema`
# MAGIC views and tag panels as the governed tags created above.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Editing, Deleting, and Un-Governing Tags
# MAGIC
# MAGIC Editing a governed tag's description or allowed values, and deleting the governed tag
# MAGIC policy itself, both require `MANAGE` permission and both happen in the account console
# MAGIC (**Governed tags** -> select the tag -> **Edit**, or **Delete**). Deleting a governed tag
# MAGIC does not remove it from the objects it was applied to; the key and value remain, but the
# MAGIC key becomes an ordinary, ungoverned tag that anyone with tag-assignment permission can
# MAGIC edit going forward.
# MAGIC
# MAGIC Removing a tag *assignment* from a specific object, as opposed to deleting the tag policy,
# MAGIC is plain SQL and works the same whether the key is governed or not:

# COMMAND ----------

# MAGIC %sql
# MAGIC ALTER TABLE testing.default.marketing_campaigns UNSET TAGS ('team');

# COMMAND ----------

# MAGIC %md
# MAGIC ## Best Practices
# MAGIC
# MAGIC 1. **Start small and scale gradually.** Pilot with a handful of high-value keys such as
# MAGIC    `sensitivity`, `costcenter`, `owner`, and `domain` before governing everything.
# MAGIC 2. **Inventory existing tags first.** The `information_schema` queries above are exactly
# MAGIC    that inventory step.
# MAGIC 3. **Define owners and stewards** for each governed tag, and grant them `CREATE` /
# MAGIC    `MANAGE` rather than leaving those permissions account-wide.
# MAGIC 4. **Communicate changes to teams** before enforcing allowed values, so existing
# MAGIC    pipelines and dashboards are not surprised by a rejected tag assignment.
# MAGIC 5. **Use tags in governance and security policies**, for example combining `sensitivity`
# MAGIC    with Data Classification and ABAC for dynamic access decisions.
# MAGIC 6. **Automate where possible**, as shown in the backfill cell above.
# MAGIC 7. **Audit regularly** for drift, using the naming-drift query above.
# MAGIC 8. **Monitor adoption metrics**, using the coverage query above.
# MAGIC 9. **Prepare for future asset types** (dashboards, notebooks) as governed tag coverage
# MAGIC    expands beyond catalogs, schemas, tables, and columns.
# MAGIC 10. **Document and share the tagging taxonomy** so new teams onboard onto the same keys
# MAGIC     and values rather than inventing their own.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Cleanup
# MAGIC
# MAGIC Drop the sample tables and remove the tag assignments made on the schema and catalog.
# MAGIC This does not remove the governed tag policies or permission grants created manually in
# MAGIC the account console; delete those there directly (**Governed tags** -> select the tag ->
# MAGIC **Delete**) if you no longer want them.

# COMMAND ----------

spark.sql("DROP TABLE IF EXISTS testing.default.customer_orders")
spark.sql("DROP TABLE IF EXISTS testing.default.employee_expenses")
spark.sql("DROP TABLE IF EXISTS testing.default.marketing_campaigns")

spark.sql("ALTER SCHEMA testing.default UNSET TAGS ('costcenter')")
spark.sql("ALTER CATALOG testing UNSET TAGS ('owner')")

print("Sample tables dropped and schema/catalog tag assignments removed.")

# COMMAND ----------

# MAGIC %md
# MAGIC **Notes:** Governed Tags were in Public Preview at the time of writing. Tag keys are
# MAGIC case sensitive. Defining governed tag policies and granting `CREATE` / `MANAGE` /
# MAGIC `ASSIGN` permissions can only be done in the account console or Catalog Explorer UI, not
# MAGIC from a notebook; see the Manual Setup Required section above. Everything else in this
# MAGIC notebook, applying tags, migrating pre-existing tags, and reading tags back through
# MAGIC `information_schema`, runs as ordinary SQL against the `testing.default` sandbox
# MAGIC catalog and schema. Permission and policy changes made in the account console can take
# MAGIC up to roughly 30 seconds (occasionally longer) to propagate.
