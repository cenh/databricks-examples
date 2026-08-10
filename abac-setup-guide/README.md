**Article:** [ABAC in Databricks Unity Catalog: A Step-by-Step Guide to Row Filters and Column Masks](https://medium.com/@cralle/a-step-by-step-guide-to-setting-up-abac-in-databricks-unity-catalog-8266454bc87f?sk=f6ae1e1a285d4bd2b04a8ebf1225cc8d)

# ABAC in Databricks Unity Catalog: A Step-by-Step Guide to Row Filters and Column Masks

A step-by-step walkthrough of setting up Attribute-Based Access Control (ABAC) in Databricks Unity Catalog, now generally available. The notebook builds a small CH Enterprise `clients` table with US and EU records, tags its sensitive columns with governed tags, and defines a row filter policy that hides EU customer rows plus a column mask policy that redacts SSNs, showing both the Catalog Explorer path and the `CREATE POLICY` SQL statement for each. It then verifies the enforced behavior and cleans up every object it created.

Creating the governed tags themselves is an account-console-only operation, so this repo pairs the runnable SQL with a detailed manual setup section for the one part that cannot be scripted.

## Files

- `abac_setup_guide_notebook.py` - Databricks notebook (SQL/Python) covering sample data setup, column tagging, the row filter and column mask UDFs, the matching ABAC policies (UI and `CREATE POLICY` SQL), verification, and cleanup.

## Requirements

- Unity Catalog enabled workspace
- Databricks Runtime 16.4+ or serverless compute (required to read ABAC-protected tables)
- Account admin access to create governed tags in the account console
- `MANAGE` privilege (or equivalent `CREATE FUNCTION` / `CREATE TABLE` / `SELECT`) on the target catalog and schema, plus permission to create policies

## Setup

Run the notebook's Setup cells to create the `testing` catalog and `default` schema (if they do not already exist) and a sample `clients` table: eight CH Enterprise client records, four in the US and four in the EU, each with an address, an SSN, and a region. These are the rows the rest of the notebook tags, filters, and masks.

## Manual setup required

Governed tags (the `pii` and `geo_region` tag keys, their allowed values, and who may assign them) are account-level objects, defined once per account rather than per workspace, and there is no SQL or PySpark statement that creates one. Before running the tagging cell in the notebook, an account admin needs to:

1. Open **Data**, then **Governance**, then **Governed Tags** in the workspace sidebar (or the account console's **Governance** section).
2. Click **Create governed tag** and define the key `pii`, with `ssn` and `address` as allowed values.
3. Click **Create governed tag** again and define the key `geo_region`, with `region` as an allowed value.
4. Set who is permitted to create, manage, and assign each tag, and save both.

The notebook's `ALTER ... SET TAGS` statements will still run without this step, they just apply as ordinary, ungoverned tags until the governed tag policy exists. Everything after that, tagging the columns, creating the row filter and column mask UDFs, and creating the ABAC policies with `CREATE POLICY`, is expressible in SQL and runs directly in the notebook.

## Cleanup

Run the notebook's Cleanup cells to drop the `hide_eu_customers` and `mask_ssn` policies, the `non_eu_region` and `mask_ssn` functions, and the sample `clients` table. This does not delete the `pii` and `geo_region` governed tag definitions created manually in the account console; remove those there directly (**Governed Tags** -> select the tag -> **Delete**) if you no longer want them.
