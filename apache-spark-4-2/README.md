**Article:** [Apache Spark 4.2: What Data Engineers Need to Know About Auto CDC and Metric Views](https://medium.com/@cralle/what-developers-need-to-know-about-apache-spark-4-2-bcc70f2c7c7d?sk=0669d6d830361919661f31a2e1bc02bc)

# Apache Spark 4.2: What Data Engineers Need to Know About Auto CDC and Metric Views

Apache Spark 4.2 moves change data capture into the engine: Auto CDC in Spark Declarative Pipelines replaces hand-written `MERGE INTO` logic, a new `CHANGES` clause standardizes reading row-level change feeds across connectors, and Data Source V2 gains a first-class CDC API underneath both. On top of that, this covers metric views (a native semantic layer), Real-Time Mode reaching PySpark, Arrow-first Python UDFs, and the SQL and platform highlights worth knowing.

## Files

- `spark_4_2_notebook.py` — the main companion notebook: sample data setup, the `CHANGES` clause, `table_changes()`, `INSERT ... BY NAME` schema evolution, metric views, Arrow-optimized UDFs, `QUALIFY`, and native geospatial types. Runs interactively.
- `auto_cdc_pipeline.py` — the Auto CDC (Spark Declarative Pipelines) source file referenced by the main notebook. This does **not** run interactively; attach it as the source of a Lakeflow pipeline (serverless, or Pro/Advanced edition) to run it.

## Requirements

- Databricks Runtime 19 (Beta) or later, which ships Apache Spark 4.2
- For the Auto CDC section: a Lakeflow Declarative Pipeline (serverless, or Pro/Advanced edition)
