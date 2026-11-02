# Databricks notebook source
# MAGIC %md
# MAGIC # Apache Spark 4.3: What Developers Need to Know
# MAGIC
# MAGIC **Article:** [Apache Spark 4.3: What Developers Need to Know](https://medium.com/@cralle/1896a29f1538?sk=9da546846112e07966435aa60a289207)
# MAGIC
# MAGIC Companion notebook for the Medium article. It walks through the Apache Spark 4.3.0 features covered there, using a sample company called CH Enterprise:
# MAGIC
# MAGIC 1. `ASOF JOIN` (behind a flag)
# MAGIC 2. Python UDFs transpiled to Catalyst expressions (experimental, behind a flag)
# MAGIC 3. `JSON_TABLE` and `UNNEST ... WITH ORDINALITY`
# MAGIC 4. Editing `VARIANT` values in place
# MAGIC 5. Nanosecond timestamps (preview, behind a flag)
# MAGIC 6. Smaller additions: `counter_diff`, `hmac`, `jaro_winkler_similarity`, `DataFrame.zip`
# MAGIC 7. Auto CDC with SCD Type 2 in Spark Declarative Pipelines (shown as pipeline source, not run here)
# MAGIC
# MAGIC **Requirements:** Apache Spark 4.3.0 (for example `pip install "pyspark[sql]==4.3.0"`), Java 17, 21, or 25, Python 3.11+, pandas below 3.0. Everything uses temporary views, so no catalog or table setup is needed. On a Databricks runtime that ships Apache Spark 4.3, the cells run as they are (at the time of writing, Databricks Runtime 19 ships Spark 4.2); set `CATALOG`/`SCHEMA` below if you want to persist anything.

# COMMAND ----------

import pyspark.sql.functions as F
from pyspark.sql import SparkSession

spark = SparkSession.builder.getOrCreate()

CATALOG, SCHEMA = "cenh_testing", "default"  # only used if you persist results

# Pin the session time zone so the timestamp outputs match the article.
spark.conf.set("spark.sql.session.timeZone", "UTC")

print("Spark version:", spark.version)
assert spark.version.startswith("4.3"), "This notebook needs Apache Spark 4.3"

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. ASOF JOIN: price every order with the latest FX rate
# MAGIC
# MAGIC `ASOF JOIN` is new SQL syntax in Spark 4.3 and ships disabled. Turn it on per session.

# COMMAND ----------

spark.conf.set("spark.sql.join.asofJoin.enabled", "true")

spark.sql("""
CREATE OR REPLACE TEMP VIEW orders AS
SELECT * FROM VALUES
  (1001, 'EUR', TIMESTAMP '2026-10-30 09:00:00', 120.00),
  (1002, 'EUR', TIMESTAMP '2026-10-30 10:15:00',  80.00),
  (1003, 'GBP', TIMESTAMP '2026-10-30 10:20:00', 200.00),
  (1004, 'SEK', TIMESTAMP '2026-10-30 10:30:00', 950.00)
AS t(order_id, currency, order_ts, amount)
""")

spark.sql("""
CREATE OR REPLACE TEMP VIEW fx_rates AS
SELECT * FROM VALUES
  ('EUR', TIMESTAMP '2026-10-30 08:00:00', 1.162),
  ('EUR', TIMESTAMP '2026-10-30 10:00:00', 1.158),
  ('GBP', TIMESTAMP '2026-10-30 09:30:00', 1.334),
  ('GBP', TIMESTAMP '2026-10-30 10:25:00', 1.336)
AS r(currency, rate_ts, usd_rate)
""")

# COMMAND ----------

priced = spark.sql("""
SELECT o.order_id, o.currency, o.order_ts, r.rate_ts, r.usd_rate,
       ROUND(o.amount * r.usd_rate, 2) AS amount_usd
FROM orders AS o
LEFT ASOF JOIN fx_rates AS r
  MATCH_CONDITION (o.order_ts >= r.rate_ts)
  ON o.currency = r.currency
ORDER BY o.order_id
""")
priced.show(truncate=False)

# COMMAND ----------

# The physical plan uses the new SortMergeAsOfJoin operator: one shuffle per side, no range join, no window.
priced.explain()

# COMMAND ----------

# Gotcha: without ON, the match only looks at time, so a GBP order can be priced with a EUR rate.
spark.sql("""
SELECT o.order_id, o.currency, r.currency AS rate_currency, r.rate_ts
FROM orders AS o
ASOF JOIN fx_rates AS r
  MATCH_CONDITION (o.order_ts >= r.rate_ts)
ORDER BY o.order_id
""").show(truncate=False)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Python UDFs that run as SQL expressions
# MAGIC
# MAGIC With `spark.sql.experimental.optimizer.transpilePyUDFs` on (and ANSI mode, the default), Spark 4.3 rewrites simple, deterministic Python UDFs into Catalyst expressions at definition time. Only row-at-a-time (non-Arrow) UDFs are rewritten, so pass `useArrow=False`.

# COMMAND ----------

spark.conf.set("spark.sql.experimental.optimizer.transpilePyUDFs", "true")


def net_amount(amount: float, discount: float) -> float:
    return amount * (1 - discount)


net_amount_udf = F.udf(net_amount, "double", useArrow=False)

order_lines = spark.createDataFrame(
    [(1001, 120.0, 0.10), (1002, 80.0, 0.00), (1003, 200.0, 0.25)],
    "order_id INT, amount DOUBLE, discount DOUBLE",
)

with_net = order_lines.select("order_id", net_amount_udf("amount", "discount").alias("net"))
with_net.explain()
with_net.show()

# COMMAND ----------

# Without the flag, the same row-at-a-time UDF runs in a Python worker (BatchEvalPython in the plan).
# Transpilation is decided when the UDF is defined, so define it after changing the flag.
spark.conf.set("spark.sql.experimental.optimizer.transpilePyUDFs", "false")
net_amount_python_udf = F.udf(net_amount, "double", useArrow=False)
order_lines.select("order_id", net_amount_python_udf("amount", "discount").alias("net")).explain()
spark.conf.set("spark.sql.experimental.optimizer.transpilePyUDFs", "true")

# COMMAND ----------

# The same UDF with the default Arrow path stays a Python UDF (ArrowEvalPython in the plan).
net_amount_arrow_udf = F.udf(net_amount, "double")
order_lines.select("order_id", net_amount_arrow_udf("amount", "discount").alias("net")).explain()

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. JSON_TABLE and UNNEST WITH ORDINALITY

# COMMAND ----------

spark.sql("""
CREATE OR REPLACE TEMP VIEW order_payloads AS
SELECT * FROM VALUES
  (1001, '{"customer":"Nordic Bikes","items":[{"sku":"CH-100","qty":2,"price":45.0},{"sku":"CH-220","qty":1,"price":30.0}]}'),
  (1002, '{"customer":"Baltic Tools","items":[{"sku":"CH-310","qty":4,"price":20.0}]}'),
  (1003, 'not json')
AS p(order_id, payload)
""")

# LATERAL is required to reference a column of the outer table. Malformed JSON (order 1003)
# is skipped by the default NULL ON ERROR; add ERROR ON ERROR after COLUMNS (...) to fail instead.
spark.sql("""
SELECT p.order_id, li.line, li.sku, li.qty, li.price
FROM order_payloads AS p,
     LATERAL JSON_TABLE(p.payload, '$.items[*]'
       COLUMNS (line FOR ORDINALITY,
                sku STRING PATH '$.sku',
                qty INT PATH '$.qty',
                price DOUBLE PATH '$.price')) AS li
ORDER BY p.order_id, li.line
""").show()

# COMMAND ----------

spark.sql("""
SELECT * FROM UNNEST(array('triage', 'refund', 'escalate')) WITH ORDINALITY AS steps(step, position)
""").show()

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Editing VARIANT values in place

# COMMAND ----------

spark.sql("""
CREATE OR REPLACE TEMP VIEW customer_profiles AS
SELECT parse_json('{"name":"Nordic Bikes","tier":"silver","legacy_id":"X-77","fax":null,"tags":["eu"]}') AS profile
""")

spark.sql("""
SELECT
  variant_set(profile, '$.tier', 'gold')              AS upgraded,
  variant_delete(profile, '$.legacy_id')              AS cleaned,
  variant_array_append(profile, '$.tags', 'b2b')      AS tagged,
  variant_strip_nulls(profile)                        AS compact
FROM customer_profiles
""").show(truncate=False)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5. Nanosecond timestamps (preview)

# COMMAND ----------

spark.conf.set("spark.sql.timestampNanosTypes.enabled", "true")

spark.sql("""
SELECT
  CAST('2026-10-30 09:30:00.123456789' AS TIMESTAMP_NTZ(9)) AS event_ts,
  unix_nanos(CAST('2026-10-30 09:30:00.123456789' AS TIMESTAMP_NTZ(9))) AS event_nanos,
  timestamp_nanos(1793352600123456789) AS from_epoch_nanos
""").show(truncate=False)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 6. Smaller additions worth knowing

# COMMAND ----------

# counter_diff turns cumulative counters into deltas and returns NULL on a reset.
spark.sql("""
SELECT service, ts, requests_total,
       counter_diff(requests_total) OVER (PARTITION BY service ORDER BY ts) AS requests
FROM VALUES
  ('checkout', TIMESTAMP '2026-10-30 09:00:00', 1000),
  ('checkout', TIMESTAMP '2026-10-30 09:01:00', 1180),
  ('checkout', TIMESTAMP '2026-10-30 09:02:00', 1420),
  ('checkout', TIMESTAMP '2026-10-30 09:03:00',   60),
  ('checkout', TIMESTAMP '2026-10-30 09:04:00',  250)
AS t(service, ts, requests_total)
ORDER BY ts
""").show()

# COMMAND ----------

customers = spark.createDataFrame(
    [("CH Enterprise", "C.H. Enterprises"), ("Nordic Bikes", "Nordik Bikes"), ("Baltic Tools", "Atlantic Tools")],
    "crm_name STRING, invoice_name STRING",
)

customers.select(
    "crm_name",
    "invoice_name",
    F.round(F.jaro_winkler_similarity("crm_name", "invoice_name"), 3).alias("similarity"),
    F.hex(F.hmac(F.lit("webhook-secret"), F.col("crm_name"))).alias("hmac_sha256"),
).show(truncate=False)

# COMMAND ----------

# DataFrame.zip combines two projections of the same source side by side, without a join key.
base = spark.createDataFrame([(1001, 120.0), (1002, 80.0)], "order_id INT, amount DOUBLE")
left = base.select("order_id")
right = base.select((F.col("amount") * 1.25).alias("amount_with_vat"))
left.zip(right).show()

# COMMAND ----------

# MAGIC %md
# MAGIC ## 7. Auto CDC with SCD Type 2 (Spark Declarative Pipelines)
# MAGIC
# MAGIC This code belongs in a pipeline source file run with the `spark-pipelines` CLI (`pip install "pyspark[pipelines]==4.3.0"`), not in an interactive session. It is shown here for reference. Auto CDC writes with `MERGE`, so the target needs a table format that supports row-level operations; on the default Parquet catalog it fails with `AUTOCDC_TARGET_DOES_NOT_SUPPORT_MERGE`.
# MAGIC
# MAGIC ```python
# MAGIC from pyspark import pipelines as dp
# MAGIC
# MAGIC dp.create_streaming_table("customers")
# MAGIC
# MAGIC dp.create_auto_cdc_flow(
# MAGIC     target="customers",
# MAGIC     source="customers_cdc_feed",
# MAGIC     keys=["customer_id"],
# MAGIC     sequence_by="event_ts",
# MAGIC     apply_as_deletes="op = 'DELETE'",
# MAGIC     except_column_list=["op", "event_ts"],
# MAGIC     stored_as_scd_type=2,
# MAGIC     track_history_column_list=["email", "tier"],
# MAGIC )
# MAGIC ```
# MAGIC
# MAGIC The SQL equivalent:
# MAGIC
# MAGIC ```sql
# MAGIC CREATE STREAMING TABLE customers;
# MAGIC
# MAGIC CREATE FLOW customers_cdc AS AUTO CDC INTO customers
# MAGIC FROM STREAM(customers_cdc_feed)
# MAGIC KEYS (customer_id)
# MAGIC APPLY AS DELETE WHEN op = 'DELETE'
# MAGIC SEQUENCE BY event_ts
# MAGIC COLUMNS * EXCEPT (op, event_ts)
# MAGIC STORED AS SCD TYPE 2
# MAGIC TRACK HISTORY ON (email, tier);
# MAGIC ```

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC **Notes:** ASOF JOIN ships disabled behind an internal flag, nanosecond timestamps are a preview behind a flag, and Python UDF transpilation is experimental. Behavior can change in later releases.
# MAGIC
# MAGIC **Sources:** [Spark 4.3.0 release notes](https://spark.apache.org/releases/spark-release-4-3-0.html), [SQL migration guide](https://spark.apache.org/docs/latest/sql-migration-guide.html), [PySpark upgrade guide](https://spark.apache.org/docs/latest/api/python/migration_guide/pyspark_upgrade.html), and the function reference shipped with Spark 4.3.0 (`DESCRIBE FUNCTION EXTENDED <name>`).
