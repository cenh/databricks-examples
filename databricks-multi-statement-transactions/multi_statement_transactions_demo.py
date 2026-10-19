# Databricks notebook source
# MAGIC %md
# MAGIC # Multi-statement transactions on Databricks: all tables or none
# MAGIC
# MAGIC **Article:** [Databricks Multi-Statement Transactions Are GA: All Tables or None](https://medium.com/@cralle/databricks-multi-statement-transactions-5175d2c17dd1?sk=e5fb728f104ca1b20ea270b5e0f719cf)
# MAGIC
# MAGIC Multi-statement transactions are Generally Available on Databricks for Unity Catalog managed Delta tables
# MAGIC (GA in the July 22, 2026 release notes, announced on the Databricks blog on October 16, 2026).
# MAGIC Several SQL statements, across several tables, now commit together or roll back together.
# MAGIC
# MAGIC This notebook follows CH Enterprise, a company that sells office furniture. Every order batch writes
# MAGIC three tables: `ch_orders`, `ch_inventory`, and `ch_load_log`. Without a transaction, a job that dies
# MAGIC between those writes leaves orders without a stock update. With one, the batch lands everywhere or nowhere.
# MAGIC
# MAGIC 1. Turn on catalog commits (the requirement for writing inside a transaction)
# MAGIC 2. Publish a batch with write-audit-publish in one `BEGIN ATOMIC` block
# MAGIC 3. Simulate a crash between two writes, and confirm the first write rolled back
# MAGIC 4. Reject a batch that would oversell, and confirm nothing changed
# MAGIC 5. Rerun a published batch, and see the idempotency audit stop it
# MAGIC 6. Inspect the Delta history: one commit per table per transaction
# MAGIC 7. Interactive transactions with `BEGIN TRANSACTION` and `ROLLBACK` (SQL warehouse only)
# MAGIC
# MAGIC **Requirements**
# MAGIC - Unity Catalog managed Delta tables with catalog commits enabled (done in section 1)
# MAGIC - Sections 1 to 6: a SQL warehouse, serverless compute, or a cluster on Databricks Runtime 18 LTS or above
# MAGIC   (`BEGIN ATOMIC` runs on 18.0, but `GET DIAGNOSTICS ... ROW_COUNT` needs 18 LTS, which is newer than 18.0 to 18.2)
# MAGIC - Section 7: a SQL warehouse (interactive transactions only run there)
# MAGIC - `CREATE TABLE` on `testing.default` (change the catalog and schema below to match yours)
# MAGIC
# MAGIC Every cell is SQL, so a SQL warehouse runs all sections. Run the cells in order. One cell each in
# MAGIC sections 3, 4, and 5 is expected to fail, and each has an **Expected to fail** note right above it.
# MAGIC "Run all" stops at the first one: click **Run all below** on the next cell to continue.

# COMMAND ----------

# MAGIC %sql
# MAGIC USE CATALOG testing;
# MAGIC USE SCHEMA default;

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Turn on catalog commits
# MAGIC Only Unity Catalog managed tables with **catalog commits** (the `catalogManaged` table feature) can be
# MAGIC written inside a transaction. Set it when you create a table, or later with `ALTER TABLE ... SET TBLPROPERTIES`
# MAGIC (Databricks Runtime 18.0 or above). You cannot toggle it with `CREATE OR REPLACE TABLE`.
# MAGIC
# MAGIC `ch_orders` plays an existing table: it is created without the feature and upgraded with `ALTER TABLE`.
# MAGIC Deletion vectors are on everywhere: together with an unpartitioned table, they give `BEGIN ATOMIC`
# MAGIC row-level conflict detection.

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Rerunnable: drop the demo tables first (DDL runs outside transactions)
# MAGIC DROP TABLE IF EXISTS ch_orders;
# MAGIC DROP TABLE IF EXISTS ch_inventory;
# MAGIC DROP TABLE IF EXISTS ch_load_log;
# MAGIC DROP TABLE IF EXISTS ch_orders_staging;
# MAGIC
# MAGIC -- New tables (deletion vectors on for row-level conflict detection)
# MAGIC CREATE TABLE ch_inventory (product_id INT, product_name STRING, on_hand INT)
# MAGIC TBLPROPERTIES ('delta.feature.catalogManaged' = 'supported', 'delta.enableDeletionVectors' = 'true');
# MAGIC
# MAGIC CREATE TABLE ch_load_log (batch_id STRING, orders_loaded BIGINT, published_at TIMESTAMP)
# MAGIC TBLPROPERTIES ('delta.feature.catalogManaged' = 'supported', 'delta.enableDeletionVectors' = 'true');
# MAGIC
# MAGIC -- Only read inside the transactions, but catalog-managed like the rest
# MAGIC CREATE TABLE ch_orders_staging (batch_id STRING, order_id STRING, product_id INT, quantity INT)
# MAGIC TBLPROPERTIES ('delta.feature.catalogManaged' = 'supported', 'delta.enableDeletionVectors' = 'true');
# MAGIC
# MAGIC -- ch_orders plays an existing table: created without catalog commits, upgraded below
# MAGIC CREATE TABLE ch_orders (order_id STRING, product_id INT, quantity INT, ordered_at TIMESTAMP)
# MAGIC TBLPROPERTIES ('delta.enableDeletionVectors' = 'true');
# MAGIC
# MAGIC -- Existing managed tables
# MAGIC ALTER TABLE ch_orders SET TBLPROPERTIES ('delta.feature.catalogManaged' = 'supported');

# COMMAND ----------

# MAGIC %md
# MAGIC Check the upgrade: `catalogManaged` should appear in the `tableFeatures` column.

# COMMAND ----------

# MAGIC %sql
# MAGIC DESCRIBE DETAIL ch_orders;

# COMMAND ----------

# MAGIC %md
# MAGIC Seed the stock and stage three order batches. In production, an upstream job writes these rows: that is the
# MAGIC "write" in write-audit-publish.
# MAGIC - `b-001`: a valid batch (3 orders)
# MAGIC - `b-003`: a valid batch (1 order for 2 desks), used for the crash in section 3
# MAGIC - `b-002`: asks for 4 chairs when only 3 are left after `b-001`, used in section 4

# COMMAND ----------

# MAGIC %sql
# MAGIC INSERT INTO ch_inventory VALUES
# MAGIC   (101, 'Standing desk',   20),
# MAGIC   (102, 'Ergonomic chair',  5),
# MAGIC   (103, 'Monitor arm',      8);
# MAGIC
# MAGIC INSERT INTO ch_orders_staging VALUES
# MAGIC   ('b-001', 'o-1001', 101, 3),
# MAGIC   ('b-001', 'o-1002', 102, 2),
# MAGIC   ('b-001', 'o-1003', 103, 1),
# MAGIC   ('b-002', 'o-1004', 101, 1),
# MAGIC   ('b-002', 'o-1005', 102, 4),
# MAGIC   ('b-003', 'o-1006', 101, 2);

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Write-audit-publish in one BEGIN ATOMIC block
# MAGIC The batch is already written to `ch_orders_staging`. Two audits raise an error with `SIGNAL` if it is not
# MAGIC fit to publish. Then three writes publish it: the orders, the stock decrement, and a load log row with the
# MAGIC number of orders inserted (captured with `GET DIAGNOSTICS ... = ROW_COUNT`).
# MAGIC
# MAGIC `BEGIN ATOMIC` commits when the block finishes, and rolls back everything if any statement fails.
# MAGIC This is the same block as in the article.

# COMMAND ----------

# MAGIC %sql
# MAGIC BEGIN ATOMIC
# MAGIC   DECLARE v_batch STRING DEFAULT 'b-001';
# MAGIC   DECLARE v_rows BIGINT;
# MAGIC
# MAGIC   -- Audit 1: never publish the same batch twice
# MAGIC   IF (SELECT COUNT(*) FROM ch_load_log WHERE batch_id = v_batch) > 0 THEN
# MAGIC     SIGNAL SQLSTATE '45000' SET MESSAGE_TEXT = 'Batch already published';
# MAGIC   END IF;
# MAGIC
# MAGIC   -- Audit 2: never sell stock we do not have (unknown products count as zero)
# MAGIC   IF (SELECT COUNT(*)
# MAGIC       FROM (SELECT product_id, SUM(quantity) AS qty
# MAGIC             FROM ch_orders_staging WHERE batch_id = v_batch
# MAGIC             GROUP BY product_id) AS s
# MAGIC       LEFT JOIN ch_inventory AS i USING (product_id)
# MAGIC       WHERE s.qty > COALESCE(i.on_hand, 0)) > 0 THEN
# MAGIC     SIGNAL SQLSTATE '45000' SET MESSAGE_TEXT = 'Batch would oversell inventory';
# MAGIC   END IF;
# MAGIC
# MAGIC   -- Publish: orders, stock, and the load log commit together
# MAGIC   INSERT INTO ch_orders
# MAGIC   SELECT order_id, product_id, quantity, current_timestamp()
# MAGIC   FROM ch_orders_staging WHERE batch_id = v_batch;
# MAGIC   GET DIAGNOSTICS v_rows = ROW_COUNT;
# MAGIC
# MAGIC   MERGE INTO ch_inventory AS i
# MAGIC   USING (SELECT product_id, SUM(quantity) AS qty
# MAGIC          FROM ch_orders_staging WHERE batch_id = v_batch
# MAGIC          GROUP BY product_id) AS s
# MAGIC   ON i.product_id = s.product_id
# MAGIC   WHEN MATCHED THEN UPDATE SET i.on_hand = i.on_hand - s.qty;
# MAGIC
# MAGIC   INSERT INTO ch_load_log VALUES (v_batch, v_rows, current_timestamp());
# MAGIC END;

# COMMAND ----------

# MAGIC %md
# MAGIC Expected: 3 orders, one load log row for `b-001` with `orders_loaded = 3`, and stock at 17 desks,
# MAGIC 3 chairs, and 7 monitor arms.

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT i.product_id, i.product_name, i.on_hand, l.batch_id, l.orders_loaded,
# MAGIC        (SELECT COUNT(*) FROM ch_orders) AS orders_rows
# MAGIC FROM ch_inventory AS i
# MAGIC CROSS JOIN ch_load_log AS l
# MAGIC ORDER BY i.product_id;

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. A crash halfway leaves nothing behind
# MAGIC This is the incident transactions exist for. Batch `b-003` is valid, but the same block now fails right
# MAGIC after the orders insert, before the stock update. The extra `SIGNAL` stands in for anything that can go
# MAGIC wrong mid-job, such as a bad cast or a dropped session.

# COMMAND ----------

# MAGIC %md
# MAGIC **Expected to fail.** The cell below stops with `Simulated crash after the orders insert`. That error is
# MAGIC the point of the demo: the check cell after it shows that nothing was written. "Run all" stops here,
# MAGIC so click **Run all below** on the next cell to continue.

# COMMAND ----------

# MAGIC %sql
# MAGIC BEGIN ATOMIC
# MAGIC   DECLARE v_batch STRING DEFAULT 'b-003';
# MAGIC   DECLARE v_rows BIGINT;
# MAGIC
# MAGIC   -- Audit 1: never publish the same batch twice
# MAGIC   IF (SELECT COUNT(*) FROM ch_load_log WHERE batch_id = v_batch) > 0 THEN
# MAGIC     SIGNAL SQLSTATE '45000' SET MESSAGE_TEXT = 'Batch already published';
# MAGIC   END IF;
# MAGIC
# MAGIC   -- Audit 2: never sell stock we do not have (unknown products count as zero)
# MAGIC   IF (SELECT COUNT(*)
# MAGIC       FROM (SELECT product_id, SUM(quantity) AS qty
# MAGIC             FROM ch_orders_staging WHERE batch_id = v_batch
# MAGIC             GROUP BY product_id) AS s
# MAGIC       LEFT JOIN ch_inventory AS i USING (product_id)
# MAGIC       WHERE s.qty > COALESCE(i.on_hand, 0)) > 0 THEN
# MAGIC     SIGNAL SQLSTATE '45000' SET MESSAGE_TEXT = 'Batch would oversell inventory';
# MAGIC   END IF;
# MAGIC
# MAGIC   -- Publish: orders, stock, and the load log commit together
# MAGIC   INSERT INTO ch_orders
# MAGIC   SELECT order_id, product_id, quantity, current_timestamp()
# MAGIC   FROM ch_orders_staging WHERE batch_id = v_batch;
# MAGIC   GET DIAGNOSTICS v_rows = ROW_COUNT;
# MAGIC
# MAGIC   -- Simulated crash between the orders insert and the stock update
# MAGIC   SIGNAL SQLSTATE '45000' SET MESSAGE_TEXT = 'Simulated crash after the orders insert';
# MAGIC
# MAGIC   MERGE INTO ch_inventory AS i
# MAGIC   USING (SELECT product_id, SUM(quantity) AS qty
# MAGIC          FROM ch_orders_staging WHERE batch_id = v_batch
# MAGIC          GROUP BY product_id) AS s
# MAGIC   ON i.product_id = s.product_id
# MAGIC   WHEN MATCHED THEN UPDATE SET i.on_hand = i.on_hand - s.qty;
# MAGIC
# MAGIC   INSERT INTO ch_load_log VALUES (v_batch, v_rows, current_timestamp());
# MAGIC END;

# COMMAND ----------

# MAGIC %md
# MAGIC The orders insert ran inside the block, but it rolled back with the rest. Expected: order `o-1006` is not
# MAGIC in `ch_orders` (0 rows), and the desks are still at 17. Without the transaction, `o-1006` would be in
# MAGIC `ch_orders` with no matching stock decrement.

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC   (SELECT COUNT(*) FROM ch_orders WHERE order_id = 'o-1006') AS o1006_rows,
# MAGIC   (SELECT on_hand FROM ch_inventory WHERE product_id = 101)  AS desks_on_hand;

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. A failed audit stops the whole batch
# MAGIC Batch `b-002` holds a valid desk order and an order for 4 chairs, with only 3 in stock. The oversell audit
# MAGIC raises `Batch would oversell inventory`, and nothing from the batch lands, valid desk order included.
# MAGIC
# MAGIC A failed audit cannot record itself: anything written inside the block rolls back with it. Log rejections
# MAGIC from your orchestrator, using the `SIGNAL` message.

# COMMAND ----------

# MAGIC %md
# MAGIC **Expected to fail.** The cell below stops with `Batch would oversell inventory`. The check cell after it
# MAGIC shows that nothing from batch `b-002` landed. If you used "Run all", click **Run all below** on the next
# MAGIC cell to continue.

# COMMAND ----------

# MAGIC %sql
# MAGIC BEGIN ATOMIC
# MAGIC   DECLARE v_batch STRING DEFAULT 'b-002';
# MAGIC   DECLARE v_rows BIGINT;
# MAGIC
# MAGIC   -- Audit 1: never publish the same batch twice
# MAGIC   IF (SELECT COUNT(*) FROM ch_load_log WHERE batch_id = v_batch) > 0 THEN
# MAGIC     SIGNAL SQLSTATE '45000' SET MESSAGE_TEXT = 'Batch already published';
# MAGIC   END IF;
# MAGIC
# MAGIC   -- Audit 2: never sell stock we do not have (unknown products count as zero)
# MAGIC   IF (SELECT COUNT(*)
# MAGIC       FROM (SELECT product_id, SUM(quantity) AS qty
# MAGIC             FROM ch_orders_staging WHERE batch_id = v_batch
# MAGIC             GROUP BY product_id) AS s
# MAGIC       LEFT JOIN ch_inventory AS i USING (product_id)
# MAGIC       WHERE s.qty > COALESCE(i.on_hand, 0)) > 0 THEN
# MAGIC     SIGNAL SQLSTATE '45000' SET MESSAGE_TEXT = 'Batch would oversell inventory';
# MAGIC   END IF;
# MAGIC
# MAGIC   -- Publish: orders, stock, and the load log commit together
# MAGIC   INSERT INTO ch_orders
# MAGIC   SELECT order_id, product_id, quantity, current_timestamp()
# MAGIC   FROM ch_orders_staging WHERE batch_id = v_batch;
# MAGIC   GET DIAGNOSTICS v_rows = ROW_COUNT;
# MAGIC
# MAGIC   MERGE INTO ch_inventory AS i
# MAGIC   USING (SELECT product_id, SUM(quantity) AS qty
# MAGIC          FROM ch_orders_staging WHERE batch_id = v_batch
# MAGIC          GROUP BY product_id) AS s
# MAGIC   ON i.product_id = s.product_id
# MAGIC   WHEN MATCHED THEN UPDATE SET i.on_hand = i.on_hand - s.qty;
# MAGIC
# MAGIC   INSERT INTO ch_load_log VALUES (v_batch, v_rows, current_timestamp());
# MAGIC END;

# COMMAND ----------

# MAGIC %md
# MAGIC Expected: still 3 orders, 1 load log row, and 3 chairs.

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT 'orders' AS check_name, COUNT(*) AS value FROM ch_orders
# MAGIC UNION ALL SELECT 'load_log_rows', COUNT(*) FROM ch_load_log
# MAGIC UNION ALL SELECT 'chairs_on_hand', on_hand FROM ch_inventory WHERE product_id = 102;

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5. The load log is now trustworthy
# MAGIC The load log commits with the data, so `b-001` in the log means its orders and stock decrement are in too.
# MAGIC That makes the first audit a real idempotency check: rerunning `b-001` raises `Batch already published`
# MAGIC instead of inserting the orders twice. This is also why retrying a block after a write conflict is safe.

# COMMAND ----------

# MAGIC %md
# MAGIC **Expected to fail.** The cell below stops with `Batch already published`. The check cell after it shows
# MAGIC that `b-001` was not loaded twice. If you used "Run all", click **Run all below** on the next cell to
# MAGIC continue.

# COMMAND ----------

# MAGIC %sql
# MAGIC BEGIN ATOMIC
# MAGIC   DECLARE v_batch STRING DEFAULT 'b-001';
# MAGIC   DECLARE v_rows BIGINT;
# MAGIC
# MAGIC   -- Audit 1: never publish the same batch twice
# MAGIC   IF (SELECT COUNT(*) FROM ch_load_log WHERE batch_id = v_batch) > 0 THEN
# MAGIC     SIGNAL SQLSTATE '45000' SET MESSAGE_TEXT = 'Batch already published';
# MAGIC   END IF;
# MAGIC
# MAGIC   -- Audit 2: never sell stock we do not have (unknown products count as zero)
# MAGIC   IF (SELECT COUNT(*)
# MAGIC       FROM (SELECT product_id, SUM(quantity) AS qty
# MAGIC             FROM ch_orders_staging WHERE batch_id = v_batch
# MAGIC             GROUP BY product_id) AS s
# MAGIC       LEFT JOIN ch_inventory AS i USING (product_id)
# MAGIC       WHERE s.qty > COALESCE(i.on_hand, 0)) > 0 THEN
# MAGIC     SIGNAL SQLSTATE '45000' SET MESSAGE_TEXT = 'Batch would oversell inventory';
# MAGIC   END IF;
# MAGIC
# MAGIC   -- Publish: orders, stock, and the load log commit together
# MAGIC   INSERT INTO ch_orders
# MAGIC   SELECT order_id, product_id, quantity, current_timestamp()
# MAGIC   FROM ch_orders_staging WHERE batch_id = v_batch;
# MAGIC   GET DIAGNOSTICS v_rows = ROW_COUNT;
# MAGIC
# MAGIC   MERGE INTO ch_inventory AS i
# MAGIC   USING (SELECT product_id, SUM(quantity) AS qty
# MAGIC          FROM ch_orders_staging WHERE batch_id = v_batch
# MAGIC          GROUP BY product_id) AS s
# MAGIC   ON i.product_id = s.product_id
# MAGIC   WHEN MATCHED THEN UPDATE SET i.on_hand = i.on_hand - s.qty;
# MAGIC
# MAGIC   INSERT INTO ch_load_log VALUES (v_batch, v_rows, current_timestamp());
# MAGIC END;

# COMMAND ----------

# MAGIC %md
# MAGIC Expected: still 3 orders, 1 load log row, and 17 desks.

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT 'orders' AS check_name, COUNT(*) AS value FROM ch_orders
# MAGIC UNION ALL SELECT 'load_log_rows', COUNT(*) FROM ch_load_log
# MAGIC UNION ALL SELECT 'desks_on_hand', on_hand FROM ch_inventory WHERE product_id = 101;

# COMMAND ----------

# MAGIC %md
# MAGIC ## 6. One commit per table
# MAGIC Each successful transaction is a single entry in each table's Delta log, even when the block both reads and
# MAGIC writes that table. Expected for `ch_inventory`: version 0 `CREATE TABLE`, version 1 `WRITE` (the seed insert),
# MAGIC and version 2 for batch `b-001`. The failed blocks in sections 3 to 5 add no versions.
# MAGIC
# MAGIC The batch version's `operation` is `TRANSACTION`, not `MERGE`: the `MERGE` is listed under
# MAGIC `intermediateOperationParameters` in `operationParameters`. A monitoring query that filters history on
# MAGIC `operation = 'MERGE'` misses writes made inside a transaction.

# COMMAND ----------

# MAGIC %sql
# MAGIC DESCRIBE HISTORY ch_inventory;

# COMMAND ----------

# MAGIC %md
# MAGIC ## 7. Interactive transactions (SQL warehouse only)
# MAGIC `BEGIN TRANSACTION` keeps the transaction open until you run `COMMIT` or `ROLLBACK`, so you can look at the
# MAGIC result before you decide. Uncommitted changes are visible only to your own session.
# MAGIC
# MAGIC Two things to know:
# MAGIC - Interactive transactions run only on SQL warehouses. Attach this notebook to one for this section.
# MAGIC - They can conflict at table level (except for unconditional appends), so they conflict more often than
# MAGIC   `BEGIN ATOMIC` under concurrent writes. After a conflict, run `ROLLBACK` before the next transaction.
# MAGIC
# MAGIC Start the transaction and restock 10 chairs:

# COMMAND ----------

# MAGIC %sql
# MAGIC BEGIN TRANSACTION;
# MAGIC
# MAGIC UPDATE ch_inventory SET on_hand = on_hand + 10 WHERE product_id = 102;
# MAGIC
# MAGIC -- Visible in this session only: 13 chairs
# MAGIC SELECT product_id, product_name, on_hand FROM ch_inventory WHERE product_id = 102;

# COMMAND ----------

# MAGIC %md
# MAGIC The delivery turned out to be for a different warehouse. Roll it back:

# COMMAND ----------

# MAGIC %sql
# MAGIC ROLLBACK;
# MAGIC
# MAGIC -- Back to 3 chairs
# MAGIC SELECT product_id, product_name, on_hand FROM ch_inventory WHERE product_id = 102;

# COMMAND ----------

# MAGIC %md
# MAGIC ## Cleanup

# COMMAND ----------

# MAGIC %sql
# MAGIC DROP TABLE IF EXISTS ch_orders;
# MAGIC DROP TABLE IF EXISTS ch_inventory;
# MAGIC DROP TABLE IF EXISTS ch_load_log;
# MAGIC DROP TABLE IF EXISTS ch_orders_staging;

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC **Takeaway:** if a job writes to two or more tables per run, wrap it in `BEGIN ATOMIC` and delete the
# MAGIC cleanup code you wrote for when it fails halfway.
# MAGIC
# MAGIC **Notes**
# MAGIC - Writes inside a transaction are limited to Unity Catalog managed tables with catalog commits. Writing to
# MAGIC   managed Iceberg tables in a transaction is in Private Preview, so this notebook uses Delta.
# MAGIC - Every job that reads or writes a table with catalog commits needs Databricks Runtime 16.4 or above. Do not
# MAGIC   cancel the `ALTER TABLE` upgrade: an interrupted upgrade can lock the table for reads and writes.
# MAGIC - DDL (`CREATE`, `ALTER`, `DROP`) must run outside a transaction, which is why setup and cleanup are
# MAGIC   plain statements.
# MAGIC - A transaction can touch up to 100 tables and 100 views, time travel is not available inside one, and
# MAGIC   any transaction rolls back after 48 hours.
# MAGIC - Tables with identity columns do not support concurrent transactions.
# MAGIC - Row-level concurrency for `MERGE` is not available on dedicated clusters or AWS GovCloud.
# MAGIC
# MAGIC **Sources**
# MAGIC - [Transactions (Databricks docs)](https://docs.databricks.com/aws/en/transactions)
# MAGIC - [Transaction modes (Databricks docs)](https://docs.databricks.com/aws/en/transactions/transaction-modes)
# MAGIC - [Tutorial: Coordinate transactions across tables (Databricks docs)](https://docs.databricks.com/aws/en/transactions/tutorial)
# MAGIC - [Catalog commits (Databricks docs)](https://docs.databricks.com/aws/en/tables/features/catalog-commits)
# MAGIC - [Row-level concurrency (Databricks docs)](https://docs.databricks.com/aws/en/optimizations/isolation/row-level-concurrency)
# MAGIC - [Identity columns (Databricks docs)](https://docs.databricks.com/aws/en/delta/generated-columns#identity-columns)
# MAGIC - [ATOMIC compound statement (Databricks docs)](https://docs.databricks.com/aws/en/sql/language-manual/sql-ref-syntax-txn-begin-atomic)
# MAGIC - [GET DIAGNOSTICS statement (Databricks docs)](https://docs.databricks.com/aws/en/sql/language-manual/control-flow/get-diagnostics-stmt)
# MAGIC - [SIGNAL statement (Databricks docs)](https://docs.databricks.com/aws/en/sql/language-manual/control-flow/signal-stmt)
# MAGIC - [Transactions are generally available (Databricks blog)](https://www.databricks.com/blog/transactions-are-generally-available-advancing-warehousing-lakehouse)
# MAGIC - [Transactions are now generally available (Databricks release notes, July 22, 2026)](https://docs.databricks.com/aws/en/release-notes/product/2026/july)
