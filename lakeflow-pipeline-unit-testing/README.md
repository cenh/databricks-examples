**Article:** [Databricks Pipeline Unit Testing Finally Arrives](<paste Medium article link once published>)

# Databricks Pipeline Unit Testing Finally Arrives

Lakeflow Declarative Pipelines now support native Python unit testing (Beta) directly inside the Lakeflow Pipelines Editor: a `test_spark` fixture redirects table operations by name to an isolated, temporary schema, so you can mock inputs and run `TestPipeline.run()` against Auto CDC flows, streaming tables, expectations, and append flows, then assert on the result with standard pytest, including `assertDataFrameEqual`.

This notebook lays out the pipeline source (`users` and `counts` tables) and the matching pytest test file the article walks through, plus an interactive preview that reproduces the same mock data and aggregation logic with plain PySpark, so you can see the result without setting up a pipeline in the Editor.

## Files

- `pipeline_unit_testing_walkthrough.py` — Databricks notebook (Python) containing the pipeline source file content, the pipeline test file content, and an interactive preview with assertions you can run directly.

## Requirements

- Pipeline `Owner` permission, plus `USE CATALOG` and `CREATE SCHEMA` on the pipeline's default catalog
- The pipeline set to the `PREVIEW` channel and `Triggered` mode; Spark Connect disabled
- Tests are Python-only, even for SQL pipelines
- No special requirements for the interactive preview section, which runs as a normal notebook against `testing.default`

## Manual setup required

The actual pipeline unit testing framework (`test_spark`, `TestPipeline.active()`, `TestPipeline.run()`) only runs from inside the web-based **Lakeflow Pipelines Editor**, against a pipeline you have open there. It is not a notebook API and cannot be triggered by running cells in this notebook. To see it for real:

1. Create a Lakeflow Declarative Pipeline, set it to the `PREVIEW` channel and `Triggered` mode.
2. Add the `users` and `counts` transformations from this notebook as a pipeline source file.
3. In the Editor, click **+ (Add) > Test** to create a test file, and paste in the test code from this notebook.
4. Run an individual test with the play button in the gutter, or **Run tests in file**. Results appear in the Editor's lower panel.

The **Interactive preview** section at the end of this notebook reproduces the same mock data and aggregation with plain PySpark against regular tables, so you can run and screenshot the underlying logic without setting up a pipeline. It writes a `ch_enterprise_users_preview` table into `testing.default`; drop it manually if you want to clean up (`DROP TABLE testing.default.ch_enterprise_users_preview`).
