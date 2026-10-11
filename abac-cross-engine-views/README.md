**Article:** [Databricks ABAC Now Follows Your Data: External Engines and Views](https://medium.com/@cralle/databricks-abac-external-engines-views-09557e458889?sk=d3651728a145da381f0b7bde2398f8d9)

# Databricks ABAC Now Follows Your Data: External Engines and Views

A hands-on look at two Unity Catalog ABAC releases. **Cross-engine ABAC** (GA, October 1, 2026) lets external Apache Spark read Unity Catalog managed tables with ABAC policies, row filters, and column masks enforced by Databricks serverless compute. **ABAC on views** (Beta, September 29, 2026) lets row filter and column mask policies target views, not only tables.

The notebook has two parts, both built on a small CH Enterprise example:

1. **Part 1, cross-engine ABAC:** a managed `ch_customers` table with catalog commits, a governed `pii = email` tag on its email column, an idempotent masking function, and a schema-level column mask policy that masks emails for everyone outside `ch_governance`. It then shows the grants an external service principal needs, the external Spark 4 session config with `ServerSidePlanning.enabled`, and a query that tracks the serverless enforcement cost in `system.billing.usage`.
2. **Part 2, ABAC on views:** an untagged `ch_orders` table that stands in for a table another team owns, a view on top of it whose columns you tag, so the schema-level email mask picks the view up with no new policy, and a view-level row filter that shows `ch_eu_partners` only EU orders. It ends with `SHOW EFFECTIVE POLICIES` and a query that shows what each group sees.

## Files

- `abac_cross_engine_and_views.py` - Databricks notebook (SQL + PySpark) covering a managed table with catalog commits, `DESCRIBE DETAIL` to confirm `catalogManaged`, column tagging, the masking and row filter functions, a schema-level column mask policy, the `EXTERNAL USE SCHEMA` grants, the external Spark session config (reference only, it runs outside Databricks), the enforcement cost query, a governed view with its own tags and a view-level row filter policy, `SHOW EFFECTIVE POLICIES`, a per-group check, and cleanup.

## Requirements

- A Unity Catalog workspace where a metastore admin has turned on **External data access** (Catalog Explorer, gear icon, **Metastore**, **Details** tab)
- Databricks Runtime 16.4 or above for every job that reads or writes a table with catalog commits (18.0 or above to upgrade an existing table)
- For Part 2: the **ABAC on Views** preview, turned on by a workspace admin on the Previews page, and Databricks Runtime 19 or above (serverless works too: the notebook was validated on serverless, which reported Databricks Runtime 19.9)
- Governed tags `pii` (with the allowed value `email`) and `geo` (with the allowed value `region`), `ASSIGN` on both, and `APPLY TAG` on the objects you tag
- Account groups `ch_governance` and `ch_eu_partners`
- `MANAGE` on the schema to create policies, permission to create functions in it, and `EXECUTE` on the functions
- Read access to `system.billing.usage` for the cost cell
- For the external read only: an Apache Spark 4 cluster outside Databricks with Delta-Spark 4.1 or above and the Unity Catalog Spark connector 0.4 or above, plus a service principal with OAuth M2M credentials that the parent catalog owner has granted `EXTERNAL USE SCHEMA`

## Setup

Governed tags and account groups are account-level objects, so create them before you run the notebook:

1. Open **Governed Tags** in the workspace (or the account console's governance section). Add the value `email` to a `pii` tag (create the tag if it does not exist), create a `geo` tag with the value `region`, and make sure you can assign both.
2. Create the account groups `ch_governance` and `ch_eu_partners`. Groups you create through the workspace SCIM API are workspace-local, so use the account console or the account SCIM API.
3. Turn on the **ABAC on Views** preview. Without it, tagging the view still works, but the view-level `CREATE POLICY` fails with `UC_ABAC_UNSUPPORTED_TABLE_TYPE`.

Then run the notebook top to bottom. It creates its own objects in `testing.default`: the tables `ch_customers` and `ch_orders`, the view `ch_orders_v`, the functions `ch_mask_email` and `ch_is_eu`, and the policies `ch_pii_email` (on the schema) and `ch_eu_partners_only` (on the view). Change the catalog and schema in the cells if you use different names. The notebook is safe to rerun.

The `ch_pii_email` policy is attached to the whole schema, so it masks every column tagged `pii = email` in `testing.default`, not only the ones the notebook creates. Use a schema of its own if others work in yours.

Tag changes and group membership changes can take a few minutes to show up in query results. To see each outcome, add yourself to `ch_eu_partners` (EU rows only, masked emails) or to `ch_governance` (every row, raw emails), wait a few minutes, and rerun the last query. The cost query returns no rows until an external engine has read a governed table, and billing usage can lag by a few hours.

The external Spark cell is shown as reference code. It runs on your own Spark cluster, not in Databricks. The sample uses the AWS packages (`hadoop-aws` and the `fs.s3.impl` line). On Azure, use `org.apache.hadoop:hadoop-azure:3.4.2` and drop the `fs` line.

## Cleanup

The Cleanup cell's `DROP` statements are commented out so the outputs stay available for inspection. Uncomment them to remove the policies, the view, and the tables. They do not remove the functions, the governed tags, or the account groups. Drop the functions with `DROP FUNCTION`, and remove the tags and groups from Governed Tags and the account console if you no longer need them.
