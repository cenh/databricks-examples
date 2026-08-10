# Databricks notebook source
# =============================================================================
# Auto CDC pipeline source (Spark 4.2 / Databricks Runtime 19 Beta)
#
# This is a Lakeflow Declarative Pipeline source file. It does NOT run
# interactively. Attach it to a pipeline and run the pipeline.
#
# Setup (run ONCE from a normal notebook / SQL editor, not in this pipeline):
#
#   CREATE OR REPLACE TABLE testing.default.customers_cdc_raw
#   AS SELECT * FROM (VALUES
#     (100, 'Ada Lovelace', 'London',     'INSERT', 1),
#     (101, 'Alan Turing',  'Manchester', 'INSERT', 1),
#     (102, 'Grace Hopper', 'New York',   'INSERT', 2),
#     (101, 'Alan Turing',  'Cambridge',  'UPDATE', 3),
#     (102, NULL,           NULL,         'DELETE', 4)
#   ) AS t(customer_id, name, city, operation, sequence_num);
#
# Create the pipeline:
#   Workflows > Pipelines > Create pipeline (Lakeflow Declarative Pipeline)
#   - Serverless, OR Pro/Advanced edition (required by the Auto CDC APIs)
#   - Source code: this file
#   - Default catalog: testing   Default schema: default
#   Then click Start. The graph shows customers_changes -> customers_current.
#
# Result in testing.default.customers_current:
#   100 Ada Lovelace  London      (insert)
#   101 Alan Turing   Cambridge   (updated from Manchester)
#   102 is deleted and does not appear
# =============================================================================

from pyspark import pipelines as dp
import pyspark.sql.functions as F

# COMMAND ----------

# A streaming view over the raw change feed
@dp.view
def customers_changes():
    return spark.readStream.table("testing.default.customers_cdc_raw")

# COMMAND ----------

# The target streaming table that changes are applied into
dp.create_streaming_table("customers_current")

# Declarative SCD Type 1 CDC flow: keys, ordering, and delete handling.
# sequence_by resolves out-of-order events; apply_as_deletes tombstones deletes.
dp.create_auto_cdc_flow(
    target="customers_current",
    source="customers_changes",
    keys=["customer_id"],
    sequence_by=F.col("sequence_num"),
    apply_as_deletes=F.expr("operation = 'DELETE'"),
    except_column_list=["operation", "sequence_num"],
    stored_as_scd_type=1,
)
