**Article:** [What Developers Need to Know About Delta Lake 4.0](https://medium.com/@cralle/what-developers-need-to-know-about-delta-lake-4-0-79489eb8cf9e?sk=864633b331861d0715e6abb1870e5fab)

# What Developers Need to Know About Delta Lake 4.0

A tour of the preview release of Delta Lake 4.0, the largest release in Delta Lake's history, built on Apache Spark 4.0: Delta Connect, Coordinated Commits, the Variant data type, Type Widening, Identity Columns and Collations (both marked Coming Soon at the time of writing), UniForm reaching general availability, Delta Kernel, Delta Rust 1.0, and a handful of smaller wins.

The notebook builds a small set of CH Enterprise sample tables, runs one runnable demo per feature against that data (or against a reference-only code snippet where a feature genuinely cannot run inside a notebook cell, such as Delta Connect and Delta Kernel), and cleans everything up at the end.

## Files

- `delta_lake_4_0_notebook.py` - Databricks notebook (Python/SQL) covering Delta Connect, Coordinated Commits, the Variant type, Type Widening, Identity Columns, Collations, UniForm, Delta Kernel, Delta Rust 1.0, and the smaller "Other Notable Changes" items, with setup and cleanup cells.

## Requirements

- Unity Catalog enabled workspace with permission to create a catalog/schema (or edit the catalog and schema names in the notebook to match ones you already have).
- Databricks Runtime supporting Variant, Type Widening, Liquid Clustering, and IDENTITY/COLLATE columns (16.4 LTS or later recommended). These are Databricks platform features used here to illustrate the corresponding Delta Lake 4.0 concepts; they track a different release cadence than the open source project.
- Cluster internet access to run `%pip install deltalake` for the Delta Rust 1.0 section.
- No external data or connectors required otherwise; the notebook generates its own sample data using literal `INSERT` statements.

## Setup

Run the Setup cells at the top of the notebook. They create the `testing.default` catalog/schema if it does not already exist, then load three CH Enterprise sample tables: a customers table (Type Widening demo), a raw sensor-readings table (Variant demo), and an orders table (Coordinated Commits, generated columns, liquid clustering, and Change Data Feed demos). The Identity Columns, Collations, UniForm, and Delta Rust sections create their own small demo tables further down in the notebook, since each of those is self-contained.

## Cleanup

Run the Cleanup cells at the end of the notebook. They drop every table created during Setup and the numbered sections (customers, both sensor-readings tables, orders, the orders archive, the identity-columns demo table, and the shipments table), and remove the ephemeral local path used by the Delta Rust 1.0 demo.
