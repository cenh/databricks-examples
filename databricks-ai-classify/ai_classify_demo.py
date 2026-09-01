# Databricks notebook source
# MAGIC %md
# MAGIC # ai_classify(): text classification in one line of SQL
# MAGIC
# MAGIC `ai_classify()` is a task-specific AI Function that classifies text against
# MAGIC labels you provide. Multi-label classification works from version 2.0;
# MAGIC confidence scores and rationales require version 2.1.
# MAGIC
# MAGIC **Requirements**
# MAGIC - Databricks Runtime 15.4 LTS or above (18.2 or above recommended)
# MAGIC - Not available on Classic SQL warehouses
# MAGIC - A region that supports AI Functions
# MAGIC
# MAGIC This notebook uses `testing.default` for its sample tables and a
# MAGIC CH Enterprise product catalog as the running example.

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. First example on a single string
# MAGIC The function returns a `VARIANT`, not a string. `response` is always an array,
# MAGIC even for single-label classification, because multi-label uses the same shape.

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT ai_classify(
# MAGIC   'Slim-fit merino wool crew neck sweater, machine washable, charcoal',
# MAGIC   '["clothing", "shoes", "accessories", "furniture", "electronics"]',
# MAGIC   MAP('version', '2.1')
# MAGIC ) AS category;

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Create the sample catalog
# MAGIC A handful of CH Enterprise products as free text, including two deliberately
# MAGIC ambiguous rows (the laptop sleeve and the smart watch) and one NULL.

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE testing.default.product_catalog (
# MAGIC   product_id  INT,
# MAGIC   description STRING
# MAGIC );
# MAGIC
# MAGIC INSERT INTO testing.default.product_catalog VALUES
# MAGIC   (1, 'Slim-fit merino wool crew neck sweater, machine washable, charcoal'),
# MAGIC   (2, 'Waterproof hiking boots with vibram sole, sizes 38 to 47'),
# MAGIC   (3, 'Genuine leather sleeve for 14-inch laptops, magnetic closure'),
# MAGIC   (4, 'Oak veneer standing desk, electric height adjustment 65 to 125 cm'),
# MAGIC   (5, 'Portable Bluetooth speaker with woven fabric shell and carry strap'),
# MAGIC   (6, 'Smart watch with interchangeable leather straps and fitness tracking'),
# MAGIC   (7, 'Stainless steel water bottle, 750 ml, vacuum insulated'),
# MAGIC   (8, NULL);

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Classify the column and pull the label out
# MAGIC The colon operator wants a column to work on, so wrap the call in a CTE rather
# MAGIC than chaining `:` onto the function directly. `ai_classify()` runs per row and
# MAGIC Databricks handles parallelization and scaling, so submit the whole dataset in
# MAGIC one query rather than batching it yourself. Row 8 is NULL and returns NULL.

# COMMAND ----------

# MAGIC %sql
# MAGIC WITH scored AS (
# MAGIC   SELECT
# MAGIC     product_id,
# MAGIC     description,
# MAGIC     ai_classify(
# MAGIC       description,
# MAGIC       '["clothing", "shoes", "accessories", "furniture", "electronics"]',
# MAGIC       MAP('version', '2.1')) AS result
# MAGIC   FROM testing.default.product_catalog
# MAGIC )
# MAGIC SELECT
# MAGIC   product_id,
# MAGIC   description,
# MAGIC   result:response[0].value::STRING AS category
# MAGIC FROM scored
# MAGIC ORDER BY product_id;

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Labels with descriptions
# MAGIC The plain array form leaves overlapping categories to the model's guess. Passing
# MAGIC a JSON object instead lets you define the boundary. This is the highest-leverage
# MAGIC change you can make: a label description is a small prompt applied consistently
# MAGIC to every row. `instructions` does the same job at the whole-task level.

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT ai_classify(
# MAGIC   'Genuine leather sleeve for 14-inch laptops, magnetic closure',
# MAGIC   '{
# MAGIC     "clothing": "Garments worn on the body",
# MAGIC     "shoes": "Footwear of any kind",
# MAGIC     "accessories": "Non-electronic carried or worn items: bags, cases, belts, jewellery",
# MAGIC     "furniture": "Household or office furniture",
# MAGIC     "electronics": "Powered devices and their components"
# MAGIC   }',
# MAGIC   MAP('version', '2.1', 'instructions', 'Categorize retail product descriptions for a European catalog.')
# MAGIC ) AS category;

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5. Keep the taxonomy in a table
# MAGIC Because `labels` is just a `STRING`, it does not have to be a literal. Build the
# MAGIC JSON from a Delta table and the query picks up taxonomy changes without a redeploy.

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE testing.default.category_taxonomy (
# MAGIC   label       STRING,
# MAGIC   description STRING
# MAGIC );
# MAGIC
# MAGIC INSERT INTO testing.default.category_taxonomy VALUES
# MAGIC   ('clothing',    'Garments worn on the body'),
# MAGIC   ('shoes',       'Footwear of any kind'),
# MAGIC   ('accessories', 'Non-electronic carried or worn items: bags, cases, belts, jewellery'),
# MAGIC   ('furniture',   'Household or office furniture'),
# MAGIC   ('electronics', 'Powered devices and their components');

# COMMAND ----------

# MAGIC %sql
# MAGIC WITH scored AS (
# MAGIC   SELECT
# MAGIC     p.product_id,
# MAGIC     p.description,
# MAGIC     ai_classify(p.description, l.labels, MAP('version', '2.1')) AS result
# MAGIC   FROM testing.default.product_catalog p
# MAGIC   CROSS JOIN (
# MAGIC     SELECT to_json(map_from_entries(collect_list(struct(label, description)))) AS labels
# MAGIC     FROM testing.default.category_taxonomy
# MAGIC   ) l
# MAGIC )
# MAGIC SELECT product_id, description, result:response[0].value::STRING AS category
# MAGIC FROM scored
# MAGIC ORDER BY product_id;

# COMMAND ----------

# MAGIC %md
# MAGIC ## 6. Confidence scores and rationales
# MAGIC Version 2.1 returns a score between 0 and 1 and a one-line justification for
# MAGIC every label. The score is what turns a black box into a dial.

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT ai_classify(
# MAGIC   'Portable Bluetooth speaker with woven fabric shell and carry strap',
# MAGIC   '["clothing", "shoes", "accessories", "furniture", "electronics"]',
# MAGIC   MAP('version', '2.1', 'enableConfidenceScores', 'true', 'enableRationales', 'true')
# MAGIC ) AS result;

# COMMAND ----------

# MAGIC %md
# MAGIC ## 7. Route on confidence
# MAGIC Auto-accept above a threshold, send the rest to a review queue. Pick the
# MAGIC threshold from your own hand-labeled data, not from a blog post.

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE testing.default.product_catalog_classified AS
# MAGIC WITH scored AS (
# MAGIC   SELECT
# MAGIC     product_id,
# MAGIC     description,
# MAGIC     ai_classify(
# MAGIC       description,
# MAGIC       '["clothing", "shoes", "accessories", "furniture", "electronics"]',
# MAGIC       MAP('version', '2.1', 'enableConfidenceScores', 'true')) AS result
# MAGIC   FROM testing.default.product_catalog
# MAGIC )
# MAGIC SELECT
# MAGIC   product_id,
# MAGIC   description,
# MAGIC   result:response[0].value::STRING            AS category,
# MAGIC   result:response[0].confidence_score::DOUBLE AS confidence,
# MAGIC   CASE WHEN result:response[0].confidence_score::DOUBLE >= 0.85
# MAGIC        THEN 'auto' ELSE 'review' END          AS routing
# MAGIC FROM scored;
# MAGIC
# MAGIC SELECT * FROM testing.default.product_catalog_classified ORDER BY confidence;

# COMMAND ----------

# MAGIC %md
# MAGIC ## 8. Multi-label
# MAGIC With `multilabel` set, `response` comes back with every label that applies.
# MAGIC `variant_explode` turns that into a clean bridge table.

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT ai_classify(
# MAGIC   'Smart watch with interchangeable leather straps and fitness tracking',
# MAGIC   '{
# MAGIC     "electronics": "Powered devices and their components",
# MAGIC     "accessories": "Non-electronic carried or worn items",
# MAGIC     "clothing": "Garments worn on the body"
# MAGIC   }',
# MAGIC   MAP('version', '2.1', 'multilabel', 'true', 'enableConfidenceScores', 'true')
# MAGIC ) AS categories;

# COMMAND ----------

# MAGIC %sql
# MAGIC WITH scored AS (
# MAGIC   SELECT
# MAGIC     product_id,
# MAGIC     ai_classify(
# MAGIC       description,
# MAGIC       '["clothing", "shoes", "accessories", "furniture", "electronics"]',
# MAGIC       MAP('version', '2.1', 'multilabel', 'true', 'enableConfidenceScores', 'true')) AS result
# MAGIC   FROM testing.default.product_catalog
# MAGIC )
# MAGIC SELECT
# MAGIC   product_id,
# MAGIC   e.value:value::STRING            AS category,
# MAGIC   e.value:confidence_score::DOUBLE AS confidence
# MAGIC FROM scored,
# MAGIC LATERAL variant_explode(scored.result:response) AS e
# MAGIC ORDER BY product_id, confidence DESC;

# COMMAND ----------

# MAGIC %md
# MAGIC ## 9. From PySpark
# MAGIC `F.expr` keeps classification as one step in a larger DataFrame pipeline, and
# MAGIC the SQL you tested in a cell is the SQL that ends up in the job.

# COMMAND ----------

import pyspark.sql.functions as F

LABELS = '["clothing", "shoes", "accessories", "furniture", "electronics"]'
OPTIONS = "MAP('version', '2.1', 'enableConfidenceScores', 'true')"

classified_df = (
    spark.read.table("testing.default.product_catalog")
    .withColumn("result", F.expr(f"ai_classify(description, '{LABELS}', {OPTIONS})"))
    .withColumn("category", F.expr("result:response[0].value::STRING"))
    .withColumn("confidence", F.expr("result:response[0].confidence_score::DOUBLE"))
    .drop("result")
)

display(classified_df.orderBy("product_id"))

# COMMAND ----------

# MAGIC %md
# MAGIC There is also a native `F.ai_classify` binding that takes a Python list or dict
# MAGIC for the labels instead of a JSON string. It was added to `pyspark.sql.functions`
# MAGIC in Databricks Runtime 19 (Spark 4.2); it is absent on 18.2 and on the older
# MAGIC client that serverless still ships, so use the `F.expr` form above if this cell
# MAGIC raises an `AttributeError`.

# COMMAND ----------

if hasattr(F, "ai_classify"):
    native_df = spark.read.table("testing.default.product_catalog").select(
        "product_id",
        F.ai_classify(
            "description",
            {
                "clothing": "Garments worn on the body",
                "shoes": "Footwear of any kind",
                "accessories": "Non-electronic carried or worn items",
                "furniture": "Household or office furniture",
                "electronics": "Powered devices and their components",
            },
        ).alias("result"),
    )
    display(native_df.orderBy("product_id"))
else:
    print(
        "F.ai_classify needs Databricks Runtime 19 (Spark 4.2) or above; "
        "on older runtimes use the F.expr form in the cell above."
    )

# COMMAND ----------

# MAGIC %md
# MAGIC ## 10. Optional: check what it cost
# MAGIC `ai_classify` bills under the `AI_FUNCTIONS` product, not the `MODEL_SERVING`
# MAGIC batch inference bucket that `ai_mask` and `ai_translate` use, so a query
# MAGIC filtering on the wrong product returns nothing.

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT *
# MAGIC FROM system.billing.usage AS u
# MAGIC WHERE u.billing_origin_product = 'AI_FUNCTIONS'
# MAGIC   AND u.product_features.ai_functions.ai_function = 'AI_CLASSIFY'
# MAGIC ORDER BY u.usage_end_time DESC
# MAGIC LIMIT 50;

# COMMAND ----------

# MAGIC %md
# MAGIC ## Cleanup (optional)

# COMMAND ----------

# MAGIC %sql
# MAGIC -- DROP TABLE IF EXISTS testing.default.product_catalog;
# MAGIC -- DROP TABLE IF EXISTS testing.default.product_catalog_classified;
# MAGIC -- DROP TABLE IF EXISTS testing.default.category_taxonomy;

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC **Notes**
# MAGIC - Version 2.1 is the recommended version and the only one that returns
# MAGIC   confidence scores and rationales.
# MAGIC - The output shape changed across versions: v1 returned a bare string, v2.0
# MAGIC   returns an array of strings in `response`, v2.1 returns an array of objects
# MAGIC   with a `value` key. Pin `version` deliberately.
# MAGIC - Limits: 2 to 500 unique labels, label names 1 to 100 characters, descriptions
# MAGIC   up to 1,000 characters, and instructions up to 20,000 characters.
# MAGIC - It is probabilistic. The same input is not guaranteed to return the same label
# MAGIC   every run, and the underlying models (Apache 2.0 licensed at time of writing)
# MAGIC   can change. Re-run a labeled sample after any version change.
# MAGIC - A confidence score is the model's estimate of its own correctness, not a
# MAGIC   calibrated probability. Set thresholds against hand-labeled data.
# MAGIC
# MAGIC Sources:
# MAGIC - ai_classify function docs, Databricks
# MAGIC   (https://docs.databricks.com/aws/en/sql/language-manual/functions/ai_classify)
# MAGIC - ai_classify PySpark function docs, Databricks
# MAGIC   (https://docs.databricks.com/aws/en/pyspark/reference/functions/ai_classify)
# MAGIC - Enrich data using AI Functions, Databricks
# MAGIC   (https://docs.databricks.com/aws/en/large-language-models/ai-functions)
