**Article:** [Mastering Governed Tags in Unity Catalog: Consistency, Compliance, and Control](https://medium.com/@cralle/mastering-governed-tags-in-unity-catalog-consistency-compliance-and-control-0bd85a8599bd?sk=5cf4ab7cedf3766e04d96db571305634)

# Mastering Governed Tags in Unity Catalog: Consistency, Compliance, and Control

A walkthrough of Governed Tags in Unity Catalog, the account-level tag policies that enforce approved keys, allowed values, and assignment permissions across every workspace instead of letting each team invent its own labels for the same thing. The notebook tags a small set of sample CH Enterprise tables, migrates a pre-existing tag onto a governed policy, and reads tags back for inventory, drift, and adoption auditing.

Creating a governed tag policy itself and granting account-level `CREATE` / `MANAGE` / `ASSIGN` permissions are account-console-only operations, so this repo pairs the runnable SQL with a detailed manual setup section for the parts that cannot be scripted.

## Files

- `governed_tags_unity_catalog_notebook.py` - Databricks notebook (Python/SQL) covering tag application at the catalog/schema/table/column level, non-breaking migration of pre-existing tags, inventory/drift/adoption queries against `information_schema`, an automated backfill example, and cleanup.

## Requirements

- Unity Catalog enabled workspace
- Governed Tags enabled for the account (Public Preview at the time of writing)
- `MANAGE` privilege on the target catalog, plus `ASSIGN` permission on the governed tags used, if you complete the manual policy setup below
- Account admin access to create governed tag policies and grant tag permissions

## Setup

Run the notebook's Setup cells to create the `testing` catalog and `default` schema (if they do not already exist) and three sample tables: `customer_orders`, `employee_expenses`, and `marketing_campaigns`. These stand in for CH Enterprise data and are what the rest of the notebook tags and audits.

## Manual setup required

Governed tag **policies** (the tag key, its allowed values, and who may create/manage/assign it) are account-level objects and can only be created through the account console or Catalog Explorer UI, not from a notebook. Before the allowed-value validation in this notebook is actually enforced, an account admin needs to:

1. Open **Catalog** in the workspace sidebar, then **Governed tags** in the left panel (or the account console's **Governance** section).
2. Click **Create governed tag** and enter the tag key exactly as used in this notebook (`sensitivity`, `costcenter`, `owner`, or `team`). Tag keys are case sensitive.
3. Optionally add a description, and optionally add allowed values, for example `sensitivity`: `public`, `internal`, `confidential`, `restricted`; `costcenter`/`team`: `engineering`, `finance`, `marketing`, `sales`.
4. Click **Create**.
5. Grant permissions: either account-wide via **Governed tags** -> **Account Permissions** -> **Grant permission set** -> select principals -> check `CREATE` / `MANAGE` / `ASSIGN` -> **Save**, or per tag via the tag's own **Permissions** tab.
6. Allow up to roughly 30 seconds (occasionally longer) for the permission change to propagate.

The notebook's `ALTER ... SET TAGS` statements will still run without this step, they just apply as ordinary, ungoverned tags until the policy exists.

## Cleanup

Run the notebook's Cleanup cells to drop the three sample tables and remove the tag assignments made on the schema and catalog. This does not delete the governed tag policies or permission grants created manually in the account console; remove those there directly (**Governed tags** -> select the tag -> **Delete**) if you no longer want them.
