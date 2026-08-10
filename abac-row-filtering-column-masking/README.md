**Article:** [Stop Writing Row Filters Table by Table: ABAC Is Now GA in Unity Catalog](https://medium.com/@cralle/govern-once-protect-everywhere-abac-row-filtering-and-column-masking-is-ga-in-unity-catalog-0cfc1165db70?sk=19d73edd3ac4c11f114cc53065ac8370)

# Stop Writing Row Filters Table by Table: ABAC Is Now GA in Unity Catalog

A PySpark walkthrough for Databricks's Attribute-Based Access Control (ABAC), Governed Tags, and Agentic Data Classification, three capabilities that went GA together in Unity Catalog. Instead of wiring a row filter or column mask onto every table by hand, you define a small set of tag-driven policies once, and protection follows the data automatically as it gets tagged (manually or by the classifier).

The notebook creates a demo table, defines a governed tag taxonomy, registers a masking UDF and a row-filter UDF, applies catalog-level ABAC policies, and verifies the result from PySpark, then cleans everything up.

## Files

- `abac_walkthrough_notebook.py` — Databricks notebook (Python/SQL) covering the full pattern: governed tags, tagging objects, masking/row-filter UDFs, ABAC policies, verification, limitations, and cleanup.

## Requirements

- Unity Catalog enabled workspace
- Databricks Runtime 16.4+ or serverless compute (required to read ABAC-protected tables)
- `MANAGE` privilege on the target catalog
- Permission to create account-level governed tags (workspace/account admin, or a delegated tag policy)

## Setup

The notebook is parameterized with widgets at the top: `catalog` (default `prod_analytics`), `schema`, and `security_schema`. Set `catalog` to a Unity Catalog catalog you have `MANAGE` on before running. Run the cells top to bottom: the notebook creates a demo `transactions` table, the governed tag taxonomy, the masking and row-filter UDFs, and catalog-level ABAC policies, verifies masking and row filtering from PySpark, then the final Cleanup section drops the policies, UDFs, tag assignments, and demo table. The account-level governed tags are intentionally left in place (their `DROP` is commented out in Cleanup), since governed tags are a shared, account-level taxonomy.
