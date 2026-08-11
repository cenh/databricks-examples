# Databricks notebook source
# MAGIC %md
# MAGIC # Unit Testing for Lakeflow Declarative Pipelines (Beta)
# MAGIC
# MAGIC **Article:** [Databricks Pipeline Unit Testing Finally Arrives](<paste Medium article link once published>)
# MAGIC
# MAGIC Companion notebook for the Medium article **"Databricks Pipeline Unit Testing Finally Arrives"**.
# MAGIC
# MAGIC Pipeline unit testing runs from inside the **Lakeflow Pipelines Editor**, not as regular interactive notebook cells.
# MAGIC The cells below are laid out the way they'd exist as separate files in a pipeline project: the pipeline source
# MAGIC (`transformations.py`) and the test file (`tests/test_pipeline.py`). An optional interactive preview at the end
# MAGIC lets you see the same logic run directly in this notebook, so you can take screenshots without setting up a pipeline.
# MAGIC
# MAGIC **Requirements**: pipeline `Owner` permission, `USE CATALOG` + `CREATE SCHEMA` on the pipeline's default catalog,
# MAGIC the pipeline set to the `PREVIEW` channel in `Triggered` mode, and Spark Connect disabled.
# MAGIC
# MAGIC Catalog/schema used: `testing.default`.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 1: Update pipeline settings
# MAGIC
# MAGIC In the pipeline UI: **Settings > Advanced settings > Channel > Preview**, and set **Pipeline mode** to **Triggered**.
# MAGIC Or edit the pipeline settings JSON directly:
# MAGIC
# MAGIC ```json
# MAGIC {
# MAGIC   "continuous": false,
# MAGIC   "channel": "PREVIEW"
# MAGIC }
# MAGIC ```

# COMMAND ----------

# MAGIC %md
# MAGIC ## Pipeline source file: `transformations.py`
# MAGIC
# MAGIC Two tables under test: `users` selects and cleans up the raw source, `counts` aggregates by `user_type`
# MAGIC and counts valid (non-null) emails. Save this as a source file in your pipeline project. Like the test file
# MAGIC below, this does **not** execute as a regular notebook cell: `from pyspark import pipelines` and the `@dp.table`
# MAGIC decorators only resolve inside the Lakeflow pipeline runtime (running it as a plain cell raises
# MAGIC `PIPELINES_NOT_SUPPORTED`). The runnable version of this logic is in the interactive preview at the end.
# MAGIC
# MAGIC ```python
# MAGIC from pyspark import pipelines as dp
# MAGIC import pyspark.sql.functions as F
# MAGIC
# MAGIC
# MAGIC @dp.table
# MAGIC def users():
# MAGIC     return (
# MAGIC         spark.read.table("testing.default.ch_enterprise_users")
# MAGIC         .select("user_id", "email", "name", "user_type")
# MAGIC     )
# MAGIC
# MAGIC
# MAGIC @dp.table
# MAGIC def counts():
# MAGIC     return (
# MAGIC         spark.read.table("testing.default.users")
# MAGIC         .withColumn("valid_email", F.col("email").isNotNull())
# MAGIC         .groupBy("user_type")
# MAGIC         .agg(
# MAGIC             F.count("user_id").alias("total_count"),
# MAGIC             F.count_if("valid_email").alias("count_valid_emails"),
# MAGIC         )
# MAGIC     )
# MAGIC ```

# COMMAND ----------

# MAGIC %md
# MAGIC ## Pipeline test file: `tests/test_pipeline.py`
# MAGIC
# MAGIC Create this file with **+ (Add) > Test** in the Lakeflow Pipelines Editor (it creates the `tests` folder for you),
# MAGIC or write it directly. Run it with the play button next to each test function, or **Run tests in file** at the
# MAGIC top of the file. This file does **not** execute as a regular notebook cell; it only runs inside the Editor's
# MAGIC test runner, against the isolated `test_spark` session.
# MAGIC
# MAGIC ```python
# MAGIC import pytest
# MAGIC from pyspark.pipelines.testing import TestPipeline, test_spark
# MAGIC from pyspark.testing import assertDataFrameEqual
# MAGIC
# MAGIC test_pipeline = TestPipeline.active()
# MAGIC
# MAGIC
# MAGIC def mock_users(session):
# MAGIC     session.sql("""
# MAGIC         CREATE TABLE testing.default.ch_enterprise_users AS
# MAGIC         SELECT * FROM VALUES
# MAGIC             (1, 'alice@chenterprise.com', 'Alice', 'admin'),
# MAGIC             (2, NULL, 'Bob', 'user'),
# MAGIC             (3, 'charlie@chenterprise.com', 'Charlie', 'user'),
# MAGIC             (4, NULL, 'Dana', 'admin')
# MAGIC         AS t(user_id, email, name, user_type)
# MAGIC     """)
# MAGIC
# MAGIC
# MAGIC def test_users_row_count(test_spark):
# MAGIC     mock_users(test_spark)
# MAGIC     test_pipeline.run(test_spark, {"testing.default.users"})
# MAGIC     result = test_spark.table("testing.default.users")
# MAGIC     assert result.count() == 4
# MAGIC
# MAGIC
# MAGIC def test_counts_aggregation(test_spark):
# MAGIC     mock_users(test_spark)
# MAGIC     test_pipeline.run(
# MAGIC         test_spark,
# MAGIC         {"testing.default.users", "testing.default.counts"},
# MAGIC     )
# MAGIC     result = test_spark.table("testing.default.counts")
# MAGIC     expected = test_spark.createDataFrame(
# MAGIC         [("admin", 2, 1), ("user", 2, 1)],
# MAGIC         schema=["user_type", "total_count", "count_valid_emails"],
# MAGIC     )
# MAGIC     assertDataFrameEqual(result, expected)
# MAGIC ```

# COMMAND ----------

# MAGIC %md
# MAGIC ## Interactive preview (optional, for screenshots)
# MAGIC
# MAGIC This section is **not** part of the pipeline testing framework. It reproduces the same mock data and the same
# MAGIC aggregation logic with plain PySpark against regular tables in `testing.default`, so you can run it directly
# MAGIC in this notebook and see the result without setting up a pipeline in the Editor.

# COMMAND ----------

spark.sql("CREATE SCHEMA IF NOT EXISTS testing.default")

spark.sql("""
    CREATE OR REPLACE TABLE testing.default.ch_enterprise_users_preview AS
    SELECT * FROM VALUES
        (1, 'alice@chenterprise.com', 'Alice', 'admin'),
        (2, NULL, 'Bob', 'user'),
        (3, 'charlie@chenterprise.com', 'Charlie', 'user'),
        (4, NULL, 'Dana', 'admin')
    AS t(user_id, email, name, user_type)
""")

# COMMAND ----------

import pyspark.sql.functions as F

users_preview = (
    spark.read.table("testing.default.ch_enterprise_users_preview")
    .select("user_id", "email", "name", "user_type")
)

counts_preview = (
    users_preview
    .withColumn("valid_email", F.col("email").isNotNull())
    .groupBy("user_type")
    .agg(
        F.count("user_id").alias("total_count"),
        F.count_if("valid_email").alias("count_valid_emails"),
    )
)

display(counts_preview)

# COMMAND ----------

assert users_preview.count() == 4
assert counts_preview.filter("user_type = 'admin'").collect()[0]["count_valid_emails"] == 1

print("Preview assertions passed.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Footnotes
# MAGIC
# MAGIC - This feature is in **Beta** and only available on the pipeline's **PREVIEW** channel as of July 2026.
# MAGIC - Test isolation only covers table operations addressed **by name**. Reads/writes by path (`/Volumes/...`,
# MAGIC   `s3://...`, `abfss://...`) or through connectors (Kafka, Auto Loader) bypass isolation and act on real
# MAGIC   production systems. Do not run the full pipeline graph if it includes those.
# MAGIC - Source: [Unit testing for pipelines - Databricks documentation](https://learn.microsoft.com/en-us/azure/databricks/ldp/unit-testing)
