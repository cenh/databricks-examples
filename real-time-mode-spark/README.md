**Article:** [Sub-Second Latency in Spark: Real-Time Mode is Generally Available On Databricks](https://medium.com/@cralle/sub-second-latency-in-spark-real-time-mode-is-generally-available-d679f1d577fc?sk=2ae1956e0a4e119958640a37a2ae0777)

# Sub-Second Latency in Spark: Real-Time Mode is Generally Available On Databricks

A walkthrough of Real-Time Mode (RTM), the Structured Streaming execution mode that replaces microbatch processing with continuous data flow, pipeline scheduling, and streaming shuffle to get millisecond-level latency out of the same Spark APIs, without running a separate Flink cluster alongside Spark. The notebook builds a small, self-contained, deterministic CH Enterprise IoT sensor event table as a static illustration of the sensor payload, shows the production RTM pattern (Kafka-to-Kafka, as in the article), and then runs a real Real-Time Mode streaming query (`trigger(realTime=...)`) end-to-end.

RTM's prerequisites (classic compute, a minimum Databricks Runtime version, autoscaling disabled, Photon disabled, and a cluster-level Spark config flag) are all cluster settings and cannot be scripted from a notebook, so this repo pairs the demo with a manual setup section for the parts that must be done when creating the cluster. RTM cannot run on serverless (the `realTime` trigger is not recognized there, and standard microbatch triggers are rejected with `INFINITE_STREAMING_TRIGGER_NOT_SUPPORTED`), so run this notebook on a classic RTM-enabled cluster.

RTM also runs only over its supported streaming connectors and requires `Update` output mode. A Delta table is an exactly-once, batch-oriented source/sink and is **not** a valid RTM source or sink, and `foreachBatch` is not supported; the canonical RTM source/sink is Apache Kafka, which is why the article's example pipeline is Kafka-to-Kafka. To stay self-contained without an external Kafka cluster, the runnable streaming cell uses a built-in `rate` source shaped into the same CH Enterprise sensor schema and a `noop` sink, and reads RTM's throughput and latency directly from the streaming query's progress.

## Files

- `real_time_mode_spark_notebook.py` - Databricks notebook (Python/SQL) covering the RTM cluster prerequisites, a self-contained deterministic Delta IoT sensor table (static illustration), the production Kafka-to-Kafka RTM pattern, a runnable Real-Time Mode streaming query (`trigger(realTime=...)`) using a `rate` source and `noop` sink, inspecting RTM's progress metrics, and cleanup.

## Requirements

- Unity Catalog enabled workspace, with permission to create a catalog, schema, and volume (or an existing `testing.default` catalog/schema you can create tables and volumes in)
- A classic compute cluster (standard or dedicated access mode) on Databricks Runtime 16.4 LTS or above (18.1 recommended), configured per the "Manual setup required" section below. RTM is not supported on serverless compute or Lakeflow Spark Declarative Pipelines, so the notebook must be run on such a cluster.

## Setup

Run the notebook's Setup cell to create the `testing` catalog and `default` schema (if they do not already exist) and a managed volume, `rtm_demo`, used to hold the streaming query's checkpoint files.

## Manual setup required

Real-Time Mode is enabled at the cluster level and none of the following can be set from notebook code. Configure the cluster before running the notebook:

1. **Compute type**: classic compute with standard or dedicated access mode. Serverless clusters and Lakeflow Spark Declarative Pipelines are not supported.
2. **Databricks Runtime**: 16.4 LTS or above (18.1 recommended).
3. **Autoscaling**: disabled. RTM needs a fixed number of task slots.
4. **Photon**: disabled.
5. **Spot instances**: disabled, to avoid interrupting a long-running streaming job.
6. **Spark configuration**, added at cluster creation under Advanced options -> Spark -> Spark config:
   ```
   spark.databricks.streaming.realTimeMode.enabled true
   ```

## Cleanup

Run the notebook's Cleanup cell to stop the streaming query if it is still active, drop the demo table (`iot_sensor_events_source`), remove the checkpoint files, and drop the `rtm_demo` volume. This does not touch the `testing.default` catalog/schema, since other notebooks may reuse them, and it does not revert any cluster configuration, since none was changed by the notebook.
