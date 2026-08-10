**Article:** [Apache Spark 4.2: What Data Engineers Need to Know About Auto CDC and Metric Views](https://medium.com/@cralle/what-developers-need-to-know-about-apache-spark-4-2-bcc70f2c7c7d?sk=0669d6d830361919661f31a2e1bc02bc)

# Apache Spark 4.2: What Data Engineers Need to Know About Auto CDC and Metric Views

Apache Spark 4.2 moves change data capture into the engine: Auto CDC in Spark Declarative Pipelines replaces hand-written `MERGE INTO` logic, a new `CHANGES` clause standardizes reading row-level change feeds across connectors, and Data Source V2 gains a first-class CDC API underneath both. On top of that, this covers metric views (a native semantic layer), Real-Time Mode reaching PySpark, Arrow-first Python UDFs, and the SQL and platform highlights worth knowing.

## Files

- `spark_4_2_notebook.py` — the main companion notebook: sample data setup, the `CHANGES` clause, `table_changes()`, `INSERT ... BY NAME` schema evolution, metric views, Arrow-optimized UDFs, `QUALIFY`, and native geospatial types. Runs interactively.
- `auto_cdc_pipeline.py` — the Auto CDC (Spark Declarative Pipelines) source file referenced by the main notebook. This does **not** run interactively; attach it as the source of a Lakeflow pipeline (serverless, or Pro/Advanced edition) to run it.

## Requirements

- The interactive notebook runs on serverless (recommended) or Databricks Runtime 19 (Beta) or later, which ships Apache Spark 4.2.
- The `CHANGES ... FROM VERSION` clause is shown as reference syntax only. It is not currently supported on Unity Catalog managed Delta (`table_changes()` is used for the runnable equivalent).
- Schema evolution on the `INSERT ... BY NAME` example is requested with the `mergeSchema` write option, because `spark.databricks.delta.schema.autoMerge.enabled` cannot be set in a serverless session.
- For the Auto CDC section: a Lakeflow Declarative Pipeline (serverless, or Pro/Advanced edition).

## Setup

Run `spark_4_2_notebook.py` top to bottom on serverless. Its Setup cells create the sample tables (`orders`, `customers`, `customers_staging`, `customers_cdc_raw`) in `testing.default`; change the catalog and schema in the cells if you use different names.

To run the Auto CDC example, first run the notebook's Setup cells so `testing.default.customers_cdc_raw` exists, then create a Lakeflow Declarative Pipeline with `auto_cdc_pipeline.py` as its source, default catalog `testing`, default schema `default`, and Start it. The flow `customers_changes -> customers_current` produces the streaming table `testing.default.customers_current` with the final state 100 (London), 101 (Cambridge), and 102 removed.
