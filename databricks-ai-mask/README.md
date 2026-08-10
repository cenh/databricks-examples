**Article:** [Masking PII in One Line of SQL: Meet Databricks ai_mask()](https://medium.com/@cralle/databricks-ai-mask-pii-masking-sql-3fa6f81eb52d?sk=cdd46c9361354dd94195c8a92a1ff8c1)

# Databricks ai_mask(): Masking PII in Free Text with One Line of SQL

A hands-on look at `ai_mask()`, a task-specific AI Function (Public Preview) that calls a generative AI model to find and mask the entities you name (names, emails, phone numbers, and more) directly from SQL, with no endpoint to deploy and no model to manage.

The notebook masks single strings, builds a small table of customer reviews with PII baked into free text, masks a whole column with SQL and with PySpark, and walks through a few pitfalls worth testing before you rely on it (labels are not exhaustive by default, context around a masked entity is not removed, and results are probabilistic rather than deterministic).

## Files

- `ai_mask_demo.py` — Databricks notebook (SQL + PySpark) covering single-string masking, table setup, column masking, custom labels, persisting masked output, common pitfalls, and checking batch-inference cost.

## Requirements

- Databricks Runtime 18.2 or above
- Serverless compute (notebooks and workflows)
- A region that supports AI Functions optimized for batch inference
- Not available on Pro or Classic SQL warehouses

## Setup

Run the notebook top to bottom on serverless. It creates its own sample table (`customer_reviews`) in `testing.default` and writes a masked copy (`customer_reviews_masked`); change the catalog and schema in the cells if you use different names. The final cost-check cell reads `system.billing.usage` and scopes the query to the current workspace automatically. The Cleanup cell's `DROP` statements are commented out so the masked output is left for inspection; uncomment them to remove both tables.
