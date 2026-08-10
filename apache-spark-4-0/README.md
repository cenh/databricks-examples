**Article:** [What Developers Need to Know About Apache Spark 4.0](https://medium.com/@cralle/what-developers-need-to-know-about-apache-spark-4-0-508d0e4a5370?sk=2a635c3e28a7aa90c655d0a2da421725)

# What Developers Need to Know About Apache Spark 4.0

A SQL and PySpark walkthrough of the Apache Spark 4.0 features that matter most for day to day data engineering: SQL-defined UDFs, named and unnamed parameter markers, collations, ANSI SQL mode by default, the new VARIANT data type, the Python Data Source API, and the streaming updates (state store improvements, transformWithState, and the State Reader API). Spark 4.0 shipped in Databricks Runtime 17.0/17.1, ahead of the LTS runtime.

The notebook builds small CH Enterprise sample tables and a synthetic IoT data source, runs one runnable demo per feature against that data, and cleans everything up at the end.

## Files

- `apache_spark_4_0_notebook.py` - Databricks notebook (Python/SQL) covering SQL-defined UDFs, parameter markers, collations, ANSI SQL mode, the VARIANT data type, the Python Data Source API, and the streaming state improvements (transformWithState, State Reader API), with setup and cleanup cells.

## Requirements

- Databricks Runtime 17.0/17.1 or later (Apache Spark 4.0). Several of the syntax items covered here (SQL-defined UDFs, parameter markers, collations, VARIANT, `transformWithStateInPandas`, the State Reader API) are not available on earlier runtimes.
- Unity Catalog enabled workspace with permission to create a catalog/schema (or edit the catalog and schema names in the notebook to match ones you already have).
- No external data or connectors required; the notebook generates its own sample data using literal `INSERT` statements, a small Delta source table it writes itself for the streaming demo, and a small custom Python data source.

## Setup

Run the Setup cells at the top of the notebook. They create the `testing.default` catalog/schema if it does not already exist, then load three small CH Enterprise sample tables: an orders table (SQL UDF and parameter marker demos), an offices table (collations demo), and a raw application log table (VARIANT demo). The Python Data Source API and streaming sections generate their own sample data further down in the notebook.

## Cleanup

Run the Cleanup cell at the end of the notebook. It drops the tables and the SQL function created during Setup, drops the in-memory streaming temp view used by the `transformWithState` demo, and removes the temporary checkpoint directory used by the streaming demos.
