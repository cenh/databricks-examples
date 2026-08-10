# Databricks notebook source
# MAGIC %md
# MAGIC # ai_mask(): Masking PII in free text with one line of SQL
# MAGIC
# MAGIC **Article:** [Masking PII in One Line of SQL: Meet Databricks ai_mask()](https://medium.com/@cralle/databricks-ai-mask-pii-masking-sql-3fa6f81eb52d?sk=cdd46c9361354dd94195c8a92a1ff8c1)
# MAGIC
# MAGIC `ai_mask()` is a task-specific AI Function (Public Preview) that calls a
# MAGIC generative AI model to mask the entities you name in a string, and returns
# MAGIC the text with those entities replaced by `[MASKED]`.
# MAGIC
# MAGIC **Requirements**
# MAGIC - Databricks Runtime 18.2 or above
# MAGIC - Serverless compute (notebooks and workflows)
# MAGIC - A region that supports AI Functions optimized for batch inference
# MAGIC - Not available on Pro or Classic SQL warehouses
# MAGIC
# MAGIC This notebook uses `testing.default` for its sample table.

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. First example on a single string
# MAGIC Mask people and email addresses. Note that "New York" is a place, not one of
# MAGIC the labels we asked for, so it is left untouched.

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT ai_mask(
# MAGIC   'John Doe lives in New York. His email is john.doe@example.com.',
# MAGIC   array('person', 'email')
# MAGIC ) AS masked;

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT ai_mask(
# MAGIC   'Contact me at 555-1234 or visit us at 123 Main St.',
# MAGIC   array('phone', 'address')
# MAGIC ) AS masked;

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Create a small sample table
# MAGIC A handful of customer reviews with names, emails, and phone numbers baked
# MAGIC into the free text.

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE testing.default.customer_reviews (
# MAGIC   review_id   INT,
# MAGIC   review_text STRING
# MAGIC );
# MAGIC
# MAGIC INSERT INTO testing.default.customer_reviews VALUES
# MAGIC   (1, 'Sarah Chen loved the new blender. Reach her at sarah.chen@example.com for a quote.'),
# MAGIC   (2, 'Terrible support. I called 555-0182 three times and no one from the team called back.'),
# MAGIC   (3, 'Dr. Okafor recommended this to our clinic. Email orders to procurement@northside.org.'),
# MAGIC   (4, 'Great value. No complaints at all.'),
# MAGIC   (5, NULL);

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Mask a column with SQL
# MAGIC `ai_mask()` runs per row. Databricks handles parallelization, retries, and
# MAGIC scaling, so the same query works on a few rows or millions. Row 4 has no PII
# MAGIC and comes back unchanged; row 5 is NULL and returns NULL.

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC   review_id,
# MAGIC   review_text,
# MAGIC   ai_mask(review_text, array('person', 'email', 'phone number')) AS masked_review
# MAGIC FROM testing.default.customer_reviews
# MAGIC ORDER BY review_id;

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Labels are flexible
# MAGIC `labels` is an array of natural-language descriptions, not a fixed list. The
# MAGIC model reads the label text, so beyond common PII you can pass more specific,
# MAGIC domain-oriented labels and they are masked too. There is no published list of
# MAGIC supported labels, so test the ones you plan to use against real data.

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT ai_mask(
# MAGIC   'Ticket for project BLUEJAY, account 4471-9920.',
# MAGIC   array('project codename', 'account number')
# MAGIC ) AS masked;
# MAGIC -- "Ticket for project [MASKED], account [MASKED]."

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5. Mask from PySpark and persist a clean table
# MAGIC Calling `ai_mask()` via `F.expr` lets masking be one step in a larger DataFrame
# MAGIC pipeline. Writing the result to its own table means everything downstream
# MAGIC reads clean text instead of re-masking on every query.

# COMMAND ----------

import pyspark.sql.functions as F

masked_df = (
    spark.read.table("testing.default.customer_reviews")
    .withColumn(
        "masked_review",
        F.expr("ai_mask(review_text, array('person', 'email', 'phone number'))"),
    )
)

masked_df.write.mode("overwrite").saveAsTable(
    "testing.default.customer_reviews_masked"
)

display(spark.read.table("testing.default.customer_reviews_masked").orderBy("review_id"))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 6. Pitfalls to test
# MAGIC A few cases that surprise people. Run them and see what comes back.

# COMMAND ----------

# MAGIC %md
# MAGIC **6a. It only masks the labels you name.** Anything you forget to list
# MAGIC stays in the clear. Here the phone number survives because `'phone number'`
# MAGIC is not in the array.

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT ai_mask(
# MAGIC   'Reach Dana Lund at dana@acme.co or call 555-0182.',
# MAGIC   array('person', 'email')   -- no 'phone number'
# MAGIC ) AS masked;

# COMMAND ----------

# MAGIC %md
# MAGIC **6b. Context is not removed, only the entity.** The name is masked, but the
# MAGIC surrounding detail can still re-identify the person.

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT ai_mask(
# MAGIC   'Dr. Alice Reyes, the only cardiologist at Riverside Clinic, signed off.',
# MAGIC   array('person')
# MAGIC ) AS masked;

# COMMAND ----------

# MAGIC %md
# MAGIC ## 7. Optional: check batch inference cost
# MAGIC `ai_mask()` usage is billed as batch inference under the `MODEL_SERVING`
# MAGIC product. The query below scopes usage to the current workspace; the
# MAGIC workspace ID is read from the notebook context, so the cell runs as-is.

# COMMAND ----------

# workspace_id is a top-level column in system.billing.usage. Read the current
# workspace ID from the notebook context so there is nothing to fill in by hand.
workspace_id = (
    dbutils.notebook.entry_point.getDbutils().notebook().getContext().workspaceId().get()
)

display(spark.sql(f"""
  SELECT *
  FROM system.billing.usage u
  WHERE u.workspace_id = '{workspace_id}'
    AND u.billing_origin_product = 'MODEL_SERVING'
    AND u.product_features.model_serving.offering_type = 'BATCH_INFERENCE'
"""))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Cleanup (optional)

# COMMAND ----------

# MAGIC %sql
# MAGIC -- DROP TABLE IF EXISTS testing.default.customer_reviews;
# MAGIC -- DROP TABLE IF EXISTS testing.default.customer_reviews_masked;

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC **Notes**
# MAGIC - `ai_mask()` is in Public Preview and HIPAA compliant. It is tuned for English
# MAGIC   during the preview, and it is probabilistic: it can miss or over-mask, and
# MAGIC   is not guaranteed to mask identically every run. Validate recall on your
# MAGIC   own data for compliance-critical use.
# MAGIC - Every masked entity becomes `[MASKED]`; masked types are not distinguishable
# MAGIC   in the output.
# MAGIC - The underlying model (Apache 2.0 licensed at time of writing) may change;
# MAGIC   re-validate afterward.
# MAGIC
# MAGIC Source: ai_mask() function docs, Databricks
# MAGIC (https://docs.databricks.com/aws/en/sql/language-manual/functions/ai_mask)
