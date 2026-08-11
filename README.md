# databricks-examples

Full, runnable Databricks notebooks to go along with my Medium articles and LinkedIn posts on Databricks, Apache Spark, and Delta Lake. Each folder below is self-contained: clone the repo, open the notebook(s) for the topic you're interested in, and import them into your own Databricks workspace.

## About me

I'm Christian Hansen, a Lead Data Engineer and Databricks Champion at [Halfspace](https://halfspace.ai). I work with Databricks, Apache Spark, and Delta Lake day to day, and I write about the features and changes that actually matter for data engineers building production pipelines, not just the release-notes summary. Follow along on Medium or LinkedIn for the full write-ups; this repo is where the code lives so you can run it yourself.

- Medium: [medium.com/@cralle](https://medium.com/@cralle)
- LinkedIn: [linkedin.com/in/cenh](https://www.linkedin.com/in/cenh/)

## Examples

| Folder | Article |
| --- | --- |
| [`abac-row-filtering-column-masking`](./abac-row-filtering-column-masking) | [Stop Writing Row Filters Table by Table: ABAC Is Now GA in Unity Catalog](https://medium.com/@cralle/govern-once-protect-everywhere-abac-row-filtering-and-column-masking-is-ga-in-unity-catalog-0cfc1165db70?sk=19d73edd3ac4c11f114cc53065ac8370) |
| [`abac-setup-guide`](./abac-setup-guide) | [ABAC in Databricks Unity Catalog: A Step-by-Step Guide to Row Filters and Column Masks](https://medium.com/@cralle/a-step-by-step-guide-to-setting-up-abac-in-databricks-unity-catalog-8266454bc87f?sk=f6ae1e1a285d4bd2b04a8ebf1225cc8d) |
| [`apache-spark-4-0`](./apache-spark-4-0) | [What Developers Need to Know About Apache Spark 4.0](https://medium.com/@cralle/what-developers-need-to-know-about-apache-spark-4-0-508d0e4a5370?sk=2a635c3e28a7aa90c655d0a2da421725) |
| [`apache-spark-4-1`](./apache-spark-4-1) | [What Developers Need to Know About Apache Spark 4.1](https://medium.com/@cralle/what-developers-need-to-know-about-apache-spark-4-1-e013ccd838f8?sk=d8b6accb0402bc0c601931d677774de2) |
| [`apache-spark-4-2`](./apache-spark-4-2) | [Apache Spark 4.2: What Data Engineers Need to Know About Auto CDC and Metric Views](https://medium.com/@cralle/what-developers-need-to-know-about-apache-spark-4-2-bcc70f2c7c7d?sk=0669d6d830361919661f31a2e1bc02bc) |
| [`auto-ttl-ga`](./auto-ttl-ga) | [Auto-TTL in Databricks: Automatic Row Deletion That Actually Works](https://medium.com/@cralle/auto-ttl-in-databricks-automated-data-retention-done-properly-5ea511b45c1d?sk=105895af20d93c0c0cd4b507029229e2) |
| [`catalogs-schemas-asset-bundles`](./catalogs-schemas-asset-bundles) | [Declarative Automation Bundles: Creating Unity Catalog Catalogs and Schemas](https://medium.com/@cralle/creating-catalogs-and-schemas-with-databricks-asset-bundles-1ab0889b4803?sk=4381bc127972f517884328dd763c7f1d) |
| [`data-skew-in-spark`](./data-skew-in-spark) | [How to Fix Data Skew in Apache Spark and Databricks: AQE, Repartitioning, and Salting](https://medium.com/@cralle/handling-data-skew-in-databricks-and-pyspark-7a16dc227a09?sk=d231b048f8d17f5efd89adb14c97a9cc) |
| [`databricks-ai-mask`](./databricks-ai-mask) | [Masking PII in One Line of SQL: Meet Databricks ai_mask()](https://medium.com/@cralle/databricks-ai-mask-pii-masking-sql-3fa6f81eb52d?sk=cdd46c9361354dd94195c8a92a1ff8c1) |
| [`delta-lake-4-0`](./delta-lake-4-0) | [What Developers Need to Know About Delta Lake 4.0](https://medium.com/@cralle/what-developers-need-to-know-about-delta-lake-4-0-79489eb8cf9e?sk=864633b331861d0715e6abb1870e5fab) |
| [`delta-lake-4-1`](./delta-lake-4-1) | [What Developers Need to Know About Delta Lake 4.1](https://medium.com/@cralle/what-developers-need-to-know-about-delta-lake-4-1-f558abf85b10?sk=7326303f9ebc180b7e9dc7a781b61545) |
| [`delta-lake-4-2`](./delta-lake-4-2) | [Delta Lake 4.2: VARIANT GA, SQL Schema Evolution, and Atomic RTAS for Catalog-Managed Tables](https://medium.com/@cralle/what-developers-need-to-know-about-delta-lake-4-2-1c2b73dd2747?sk=a4d071df6d4b39083e2a28ebb447940e) |
| [`delta-lake-time-travel-changes`](./delta-lake-time-travel-changes) | [How Delta Lake Time Travel and VACUUM Retention Now Work in Databricks](https://medium.com/@cralle/important-changes-coming-to-delta-lake-time-travel-databricks-december-2025-644b6fd03d9e?sk=2a5512a5842cf798fe00d4a884d55997) |
| [`governed-tags-unity-catalog`](./governed-tags-unity-catalog) | [Mastering Governed Tags in Unity Catalog: Consistency, Compliance, and Control](https://medium.com/@cralle/mastering-governed-tags-in-unity-catalog-consistency-compliance-and-control-0bd85a8599bd?sk=5cf4ab7cedf3766e04d96db571305634) |
| [`lakeflow-pipeline-unit-testing`](./lakeflow-pipeline-unit-testing) | [Databricks Pipeline Unit Testing Finally Arrives](<paste Medium article link once published>) |
| [`native-python-unit-testing`](./native-python-unit-testing) | [Unit Testing in Databricks Notebooks: Native pytest Support in the Workspace](https://medium.com/@cralle/native-python-unit-testing-in-databricks-notebooks-29e3130a36ec?sk=5ffec8e03832fc6f108ed35620d6cd7d) |
| [`predictive-optimization`](./predictive-optimization) | [Databricks Predictive Optimization: How Automatic OPTIMIZE, VACUUM, and CLUSTER BY AUTO Really Work](https://medium.com/@cralle/demystifying-predictive-optimization-in-databricks-automate-table-tuning-with-confidence-a5bc293292c3?sk=026c78ada316bfc19b45f7f8f555b9bc) |
| [`real-time-mode-spark`](./real-time-mode-spark) | [Sub-Second Latency in Spark: Real-Time Mode is Generally Available On Databricks](https://medium.com/@cralle/sub-second-latency-in-spark-real-time-mode-is-generally-available-d679f1d577fc?sk=2ae1956e0a4e119958640a37a2ae0777) |
| [`spark-jobs-stages-tasks`](./spark-jobs-stages-tasks) | [How Apache Spark Really Runs Your Code: Jobs, Stages, and Tasks](https://medium.com/@cralle/how-spark-really-runs-your-code-a-deep-dive-into-jobs-stages-and-tasks-2b63b135df4e?sk=fec82fc46c1f817ad7abbad55715d222) |
| [`temporary-tables-databricks-sql`](./temporary-tables-databricks-sql) | [Temporary Tables in Databricks SQL: Staging Data Without Cluttering Your Catalog](https://medium.com/@cralle/temporary-tables-in-databricks-sql-a-familiar-pattern-finally-done-right-a5dcee1609a4?sk=65307b456bebc39155b04f7b05e658d8) |
| [`unity-catalog-external-locations`](./unity-catalog-external-locations) | [Managing Unity Catalog External Locations with Declarative Automation Bundles](https://medium.com/@cralle/managing-unity-catalog-external-locations-with-declarative-automation-bundles-f754263b74eb?sk=16d20730327139e996f9f4884bee71c2) |

Each folder has its own README with a short summary, the files it contains, and a link to the related article.

## License

MIT, see [LICENSE](./LICENSE).
