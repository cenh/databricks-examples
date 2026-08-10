# Databricks notebook source
# MAGIC %md
# MAGIC **Article:** [Sub-Second Latency in Spark: Real-Time Mode is Generally Available On Databricks](https://medium.com/@cralle/sub-second-latency-in-spark-real-time-mode-is-generally-available-d679f1d577fc?sk=2ae1956e0a4e119958640a37a2ae0777)
# MAGIC
# MAGIC # Sub-Second Latency in Spark: Real-Time Mode is Generally Available On Databricks
# MAGIC
# MAGIC Author: Christian Hansen (https://medium.com/@cralle)
# MAGIC
# MAGIC Published: April 25, 2026
# MAGIC
# MAGIC Real-Time Mode (RTM) brings millisecond-level latency to Spark Structured Streaming
# MAGIC using the same DataFrame and `writeStream` APIs, so teams no longer need to run a
# MAGIC separate Flink cluster alongside Spark just to get sub-second processing.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Overview
# MAGIC
# MAGIC Real-Time Mode (RTM) replaces Structured Streaming's default microbatch model, which
# MAGIC collects incoming data into periodic chunks, with a continuous data flow that processes
# MAGIC each event as it arrives. Three architectural changes make this possible:
# MAGIC
# MAGIC 1. **Continuous data flow** - data is processed as it arrives instead of being grouped
# MAGIC    into periodic batches, removing the latency cost of waiting for a batch boundary.
# MAGIC 2. **Pipeline scheduling** - stages in the query plan run simultaneously instead of
# MAGIC    blocking, so downstream tasks can start before the upstream stage finishes writing.
# MAGIC 3. **Streaming shuffle** - data moves between tasks in real time, bypassing the
# MAGIC    disk-based shuffle that traditional microbatch stages use between stages.
# MAGIC
# MAGIC RTM is a good fit for fraud detection, real-time ML feature computation, live
# MAGIC personalization, and IoT anomaly detection: cases that cannot tolerate batch delays.
# MAGIC It is not a general replacement for microbatch. Stick with microbatch for ETL that only
# MAGIC needs minute- or second-level freshness, for very high-throughput bulk workloads where
# MAGIC per-record efficiency matters more than per-event latency, and note that RTM is not
# MAGIC available at all on serverless compute or Lakeflow Spark Declarative Pipelines.
# MAGIC
# MAGIC RTM also runs a deliberately narrow set of connectors: its supported sources and sinks
# MAGIC are the streaming, at-least-once ones (Apache Kafka is the canonical choice, which is why
# MAGIC the article's example pipeline is Kafka-to-Kafka), and it requires `Update` output mode.
# MAGIC A Delta table is an exactly-once, batch-oriented source/sink and is **not** supported by
# MAGIC RTM, so a "Delta-to-Delta" RTM query cannot run (see the constraints section below). To
# MAGIC keep this notebook self-contained and runnable without an external Kafka cluster, the
# MAGIC live streaming cell uses a `rate` source (a built-in RTM-supported source) shaped into
# MAGIC the same CH Enterprise sensor schema, and a `noop` sink so we can measure RTM's continuous
# MAGIC throughput and latency directly from the query's own progress metrics.
# MAGIC
# MAGIC This notebook:
# MAGIC
# MAGIC 1. Creates the sandbox catalog, schema, and a Unity Catalog volume for the streaming
# MAGIC    checkpoint (**Setup**).
# MAGIC 2. Walks through the cluster-level prerequisites RTM needs, which cannot be set from
# MAGIC    notebook code (**Manual setup required**).
# MAGIC 3. Builds a small, self-contained, deterministic CH Enterprise IoT sensor event table as a
# MAGIC    static illustration of the sensor payload.
# MAGIC 4. Shows the production RTM pattern (Kafka-to-Kafka, as in the article) and then runs a
# MAGIC    real Real-Time Mode query (`trigger(realTime=...)`) end-to-end using RTM-supported
# MAGIC    connectors. This requires a classic RTM-enabled cluster; it does not run on serverless.
# MAGIC 5. Inspects RTM's throughput and latency from the streaming query progress.
# MAGIC 6. Stops the query and removes the demo table, checkpoint files, and volume (**Cleanup**).

# COMMAND ----------

# MAGIC %md
# MAGIC ## Setup
# MAGIC
# MAGIC Create the sandbox catalog and schema (`testing.default`) and a managed volume to
# MAGIC hold the streaming query's checkpoint files. The volume is only used for this demo's
# MAGIC checkpoint location; it stands in for wherever a real CH Enterprise pipeline would keep
# MAGIC its streaming checkpoints.

# COMMAND ----------

catalog = "testing"
schema = "default"
volume_name = "rtm_demo"

spark.sql(f"CREATE CATALOG IF NOT EXISTS {catalog}")
spark.sql(f"CREATE SCHEMA IF NOT EXISTS {catalog}.{schema}")
spark.sql(f"CREATE VOLUME IF NOT EXISTS {catalog}.{schema}.{volume_name}")
spark.sql(f"USE CATALOG {catalog}")
spark.sql(f"USE SCHEMA {schema}")

checkpoint_root = f"/Volumes/{catalog}/{schema}/{volume_name}/checkpoints"
checkpoint_location = f"{checkpoint_root}/chenterprise_rtm"
source_table = f"{catalog}.{schema}.iot_sensor_events_source"

print(f"Checkpoint location: {checkpoint_location}")
print(f"Source table:        {source_table}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Manual setup required (cluster configuration)
# MAGIC
# MAGIC RTM is enabled at the cluster level, not from notebook code. None of the settings below
# MAGIC can be applied by running a cell; an admin has to configure the cluster before creating it
# MAGIC (or edit and restart an existing one) for the real low-latency path to actually be used:
# MAGIC
# MAGIC 1. **Compute type.** Classic compute with standard or dedicated access mode. Serverless
# MAGIC    clusters and Lakeflow Spark Declarative Pipelines are not supported.
# MAGIC 2. **Databricks Runtime.** 16.4 LTS or above. DBR 18.1 is recommended for the latest RTM
# MAGIC    optimizations.
# MAGIC 3. **Autoscaling.** Must be disabled. RTM needs a fixed number of task slots to sustain
# MAGIC    low-latency scheduling; a cluster that is resizing while it processes cannot guarantee
# MAGIC    that.
# MAGIC 4. **Photon.** Must be disabled.
# MAGIC 5. **Spot instances.** Should be disabled, so an interruption does not disrupt a
# MAGIC    long-running, latency-sensitive streaming job.
# MAGIC 6. **Spark configuration.** Add the following at cluster creation, under
# MAGIC    **Advanced options -> Spark -> Spark config**:
# MAGIC    ```
# MAGIC    spark.databricks.streaming.realTimeMode.enabled true
# MAGIC    ```
# MAGIC
# MAGIC Because none of that can be scripted from here, and because the `realTime` trigger is only
# MAGIC recognized on a runtime configured for RTM, the live streaming cell further down must be run
# MAGIC on a classic RTM-enabled cluster configured as above. It will not run on serverless.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Demo payload: a deterministic CH Enterprise IoT sensor table
# MAGIC
# MAGIC Build a small, fixed Delta table of simulated readings from ten CH Enterprise IoT
# MAGIC temperature/humidity sensors, the kind of workload the article calls out as a canonical RTM
# MAGIC use case (IoT anomaly detection). Values are derived deterministically from a row id (no
# MAGIC random component). This table is a **static illustration of the sensor payload** that a real
# MAGIC pipeline would carry on a Kafka topic; it is not the RTM source itself, because RTM does not
# MAGIC support reading from a Delta table (see the constraints below). The live RTM cell reproduces
# MAGIC this exact schema from a streaming `rate` source.

# COMMAND ----------

import pyspark.sql.functions as F

source_df = (
    spark.range(0, 100)
    .withColumn("sensor_id", (F.col("id") % F.lit(10)).cast("int"))
    .withColumn("device_name", F.concat(F.lit("chenterprise-sensor-"), F.col("sensor_id")))
    .withColumn("event_time", F.expr("timestamp('2026-01-01 00:00:00') + (id * INTERVAL 1 SECOND)"))
    .withColumn("temperature_celsius", F.round(F.lit(20) + (F.col("id") % F.lit(8)) + (F.col("id") % F.lit(4)) * F.lit(0.25), 2))
    .withColumn("humidity_pct", F.round(F.lit(40) + (F.col("id") % F.lit(30)), 2))
    .select("event_time", "sensor_id", "device_name", "temperature_celsius", "humidity_pct")
)

source_df.write.mode("overwrite").saveAsTable(source_table)
print(f"Wrote {spark.table(source_table).count()} rows to {source_table}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Real-Time Mode connectors: what RTM actually supports
# MAGIC
# MAGIC RTM uses the same `readStream`/`writeStream` APIs as any Structured Streaming job, but it
# MAGIC only runs over its supported, streaming, at-least-once connectors, and it requires `Update`
# MAGIC output mode. Notably:
# MAGIC
# MAGIC - **Sources.** Streaming sources such as Apache Kafka (and the built-in `rate` source used
# MAGIC   here). A Delta table is **not** a supported RTM source
# MAGIC   (`STREAMING_REAL_TIME_MODE.INPUT_STREAM_NOT_SUPPORTED`).
# MAGIC - **Sinks.** Streaming, at-least-once sinks such as Apache Kafka (plus `noop`/`foreach` for
# MAGIC   testing). Exactly-once/batch sinks are rejected: the Delta sink is not on the RTM
# MAGIC   allowlist (`OPERATOR_OR_SINK_NOT_IN_ALLOWLIST`), the memory sink is rejected as
# MAGIC   exactly-once (`EXACTLY_ONCE_SINK_NOT_SUPPORTED`), and `foreachBatch` is not supported
# MAGIC   (`SINK_NOT_SUPPORTED`) because RTM processes records continuously rather than in batches.
# MAGIC - **Output mode.** `Append` is rejected (`OUTPUT_MODE_NOT_SUPPORTED`); RTM requires
# MAGIC   `Update`.
# MAGIC
# MAGIC In production the article's pipeline is Kafka-to-Kafka, which looks like this (shown for
# MAGIC reference; it is not executed here because it needs a running Kafka cluster):
# MAGIC
# MAGIC ```python
# MAGIC (spark.readStream
# MAGIC     .format("kafka")
# MAGIC     .option("kafka.bootstrap.servers", "<broker:9092>")
# MAGIC     .option("subscribe", "chenterprise-sensor-events")
# MAGIC     .load()
# MAGIC     .writeStream
# MAGIC     .format("kafka")
# MAGIC     .option("kafka.bootstrap.servers", "<broker:9092>")
# MAGIC     .option("topic", "chenterprise-sensor-anomalies")
# MAGIC     .option("checkpointLocation", checkpoint_location)
# MAGIC     .outputMode("update")
# MAGIC     .trigger(realTime="30 seconds")   # duration = checkpoint interval, not a batch interval
# MAGIC     .start())
# MAGIC ```

# COMMAND ----------

# MAGIC %md
# MAGIC ## Real-Time Mode streaming query (runnable)
# MAGIC
# MAGIC This is a real Real-Time Mode query, using the same `readStream`/`writeStream` APIs and a
# MAGIC `trigger(realTime=...)`. The duration passed to `realTime` is the checkpoint interval (how
# MAGIC often state and progress are persisted), not a processing interval, since RTM processes
# MAGIC events continuously rather than on a schedule.
# MAGIC
# MAGIC To stay self-contained (no external Kafka), it reads from a streaming `rate` source and
# MAGIC shapes each event into the same CH Enterprise sensor schema as the table above, then writes
# MAGIC to a `noop` sink so we can read RTM's continuous throughput and latency straight from the
# MAGIC query's progress. **This cell requires a classic RTM-enabled cluster** (see "Manual setup
# MAGIC required"); the `realTime` trigger is only recognized on a runtime configured for RTM and it
# MAGIC does not run on serverless. RTM queries run continuously, so the cell starts the query, lets
# MAGIC it process for a short while, then stops it explicitly.

# COMMAND ----------

dbutils.fs.rm(checkpoint_location, recurse=True)

sensor_stream = (
    spark.readStream.format("rate").option("rowsPerSecond", 20).load()
    .withColumn("sensor_id", (F.col("value") % F.lit(10)).cast("int"))
    .withColumn("device_name", F.concat(F.lit("chenterprise-sensor-"), F.col("sensor_id")))
    .withColumnRenamed("timestamp", "event_time")
    .withColumn("temperature_celsius", F.round(F.lit(20) + (F.col("value") % F.lit(8)) + (F.col("value") % F.lit(4)) * F.lit(0.25), 2))
    .withColumn("humidity_pct", F.round(F.lit(40) + (F.col("value") % F.lit(30)), 2))
    .select("event_time", "sensor_id", "device_name", "temperature_celsius", "humidity_pct")
)

streaming_query = (
    sensor_stream
    .writeStream
    .format("noop")
    .outputMode("update")
    .option("checkpointLocation", checkpoint_location)
    .queryName("chenterprise_rtm_demo")
    .trigger(realTime="5 seconds")
    .start()
)

# RTM is continuous: let it process for a short while, capture progress, then stop the query.
streaming_query.awaitTermination(20)
progress = streaming_query.recentProgress
if streaming_query.isActive:
    streaming_query.stop()

total_rows = sum(p.get("numInputRows", 0) for p in progress)
print(f"Real-Time Mode query complete. Progress reports: {len(progress)}, rows processed: {total_rows}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Inspecting the output
# MAGIC
# MAGIC With a `noop` sink there is no output table to query; instead we read RTM's own streaming
# MAGIC query progress, which is where the latency and throughput story lives. Each progress record
# MAGIC reports how many events were processed and how fast, so you can see the continuous,
# MAGIC sub-second processing RTM delivers before the query is stopped.

# COMMAND ----------

metrics = [
    {
        "batch_id": int(p.get("batchId") or 0),
        "num_input_rows": int(p.get("numInputRows") or 0),
        "input_rows_per_second": float(round(p.get("inputRowsPerSecond") or 0.0, 1)),
        "processed_rows_per_second": float(round(p.get("processedRowsPerSecond") or 0.0, 1)),
    }
    for p in progress
]
if metrics:
    display(spark.createDataFrame(metrics))
else:
    print("No progress records captured; try increasing the awaitTermination duration.")

# COMMAND ----------

# MAGIC %md
# MAGIC For reference, here is the shape of the static CH Enterprise sensor payload built earlier,
# MAGIC aggregated per sensor. In a production RTM pipeline this same per-sensor logic would run
# MAGIC continuously against the live Kafka feed.

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC   sensor_id,
# MAGIC   ROUND(AVG(temperature_celsius), 2) AS avg_temperature_celsius,
# MAGIC   ROUND(AVG(humidity_pct), 2) AS avg_humidity_pct,
# MAGIC   COUNT(*) AS reading_count
# MAGIC FROM testing.default.iot_sensor_events_source
# MAGIC GROUP BY sensor_id
# MAGIC ORDER BY sensor_id;

# COMMAND ----------

# MAGIC %md
# MAGIC ## Cleanup
# MAGIC
# MAGIC Stop the streaming query if it is still running, drop the demo table, remove the
# MAGIC checkpoint files, and drop the demo volume. This does not touch the `testing.default`
# MAGIC catalog/schema themselves, since other notebooks may reuse them, and it does not touch any
# MAGIC cluster configuration, since none of that was set from this notebook in the first place.

# COMMAND ----------

if streaming_query.isActive:
    streaming_query.stop()

spark.sql(f"DROP TABLE IF EXISTS {source_table}")
dbutils.fs.rm(checkpoint_root, recurse=True)
spark.sql(f"DROP VOLUME IF EXISTS {catalog}.{schema}.{volume_name}")

print("Streaming query stopped, demo table dropped, checkpoint files removed, volume dropped.")

# COMMAND ----------

# MAGIC %md
# MAGIC **Notes:** RTM's cluster-level prerequisites (classic compute, DBR 16.4 LTS+, autoscaling
# MAGIC disabled, Photon disabled, spot instances disabled, and the
# MAGIC `spark.databricks.streaming.realTimeMode.enabled` Spark config) can only be set when
# MAGIC creating or editing the cluster, not from notebook code; see "Manual setup required" above.
# MAGIC RTM cannot run on serverless (the `realTime` trigger is not recognized there, and even
# MAGIC standard microbatch triggers are rejected with `INFINITE_STREAMING_TRIGGER_NOT_SUPPORTED`), so
# MAGIC run this notebook on a classic RTM cluster. Beyond the cluster settings, RTM only runs over its
# MAGIC supported streaming connectors and requires `Update` output mode: a Delta table is not a valid
# MAGIC RTM source or sink, and `foreachBatch` is not supported, which is why the article's production
# MAGIC pipeline (and any real RTM pipeline) uses a streaming source and sink such as Apache Kafka. This
# MAGIC notebook therefore demonstrates a genuine `trigger(realTime=...)` query end-to-end using a
# MAGIC built-in `rate` source and a `noop` sink, and reads latency and throughput from the query's own
# MAGIC progress. The sub-100ms end-to-end latency figures the article quotes were measured in the
# MAGIC author's own test environment on a specific workload; treat them as directional, not as a
# MAGIC guaranteed number for every pipeline, since actual latency depends on the cluster size,
# MAGIC source/sink, and query complexity.
