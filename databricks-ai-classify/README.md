**Article:** [PLACEHOLDER - add the live Medium title and URL once the article is published]

# Databricks ai_classify(): Text Classification in One Line of SQL

A hands-on look at `ai_classify()`, the task-specific AI Function that classifies text against labels you provide. Multi-label classification works from version 2.0; confidence scores and rationales require version 2.1.

The notebook classifies single strings, builds a small CH Enterprise product catalog with free-text descriptions, classifies the whole column with SQL and with PySpark, and works through the things that decide whether this is usable in a pipeline: label descriptions instead of bare label names, keeping the taxonomy in a Delta table, routing rows on their confidence score, and multi-label output.

## Files

- `ai_classify_demo.py` - Databricks notebook (SQL + PySpark) covering single-string classification, table setup, column classification, label descriptions and global instructions, a table-driven taxonomy, confidence scores and rationales, confidence-based routing, multi-label with `variant_explode`, the `F.expr` and native `F.ai_classify` PySpark bindings, and checking cost in the billing system table.

## Requirements

- Databricks Runtime 15.4 LTS or above (18.2 or above recommended)
- Not available on Classic SQL warehouses
- A region that supports AI Functions

## Setup

Run the notebook top to bottom. It creates its own sample tables in `testing.default` (`product_catalog`, `category_taxonomy`, and the classified output `product_catalog_classified`); change the catalog and schema in the cells if you use different names.

One cell is worth knowing about before you run it. The second cell in section 9 uses the native `F.ai_classify()` binding, which was added to `pyspark.sql.functions` in Databricks Runtime 19 (Spark 4.2). It is absent on 18.2 and on the older client that serverless still ships; the cell guards on that with `hasattr`, so on older runtimes it prints a note and skips rather than erroring. Use the `F.expr` form in the cell above instead on those runtimes.

The cost-check cell reads `system.billing.usage` and filters on the `AI_FUNCTIONS` product. Note that `ai_classify` bills there rather than under the `MODEL_SERVING` batch-inference bucket used by `ai_mask` and `ai_translate`, so a query pointed at the wrong product returns nothing. The Cleanup cell's `DROP` statements are commented out so the classified output is left for inspection; uncomment them to remove all three tables.
