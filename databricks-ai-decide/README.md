**Article:** [Databricks ai_decide: The AI Judge That Never Writes a Word](https://medium.com/@cralle/38139e614702?sk=28175398b6b8ad592de4f249159fa4b1)

# Databricks ai_decide(): The AI Judge That Never Writes a Word

A hands-on look at `ai_decide()`, the Beta AI Function powered by a decision model. Instead of generating text, it answers typed questions about text or JSON and returns probabilities, choices, and scores. Three question types are supported: `noul` (yes or no, as a probability), `choice` (pick one label), and `score` (a position on an ordered scale).

The notebook has two parts, both built on a small CH Enterprise support inbox:

1. **Sections 1 to 5:** a simple triage pass to learn the output. One call per ticket decides the owning team, the churn risk, and the urgency; then a look inside a `choice` answer, and a cell that scores every ticket three times in one query to show how much the numbers move.
2. **Sections 6 to 14:** `ai_decide` as an LLM judge. `ai_query` drafts a reply to every ticket, `ai_decide` grades each draft on three questions (does it answer the ticket, does it promise a refund, a credit, or a date, and how good is it), a gate decides send, rewrite, or hold, and a count per gate gives you one number to track on every run. It ends with a threshold-calibration query against human labels, failure checks, the same judge in a DataFrame pipeline, the REST endpoint with a latency measurement, and a cost lookup in the billing system table.

## Files

- `ai_decide_demo.py` - Databricks notebook (SQL + PySpark) covering a single-string decision with all three question types, multi-question triage over a table, inspecting choice probabilities with `variant_explode`, run-to-run variation, reply drafting with `ai_query`, a three-question judge with versioned grades, a send/rewrite/hold gate with thresholds between the value steps, gate counts, threshold calibration against human labels, failure checks, the PySpark `F.expr` pattern with safe escaping and a hash of the question definitions, the REST endpoint through the Databricks SDK with timing, and AI Function usage in `system.billing.usage`.

## Requirements

- Access to the `ai_decide` Beta (workspace admins control it on the Previews page)
- Databricks Runtime 15.4 LTS or above (18.2 or above recommended)
- On serverless, environment version 2 or above
- Not available on Classic SQL warehouses
- A region that supports AI Functions
- A Foundation Model API chat endpoint for the drafts (the notebook uses `databricks-claude-haiku-4-5`)
- Read access to `system.billing.usage` for the optional cost cell

## Setup

Run the notebook top to bottom. It creates its own objects in `testing.default`: the tables `support_tickets`, `support_tickets_triaged`, `support_reply_drafts`, `support_reply_evals`, and `support_reply_labels`, plus the view `support_reply_gate`. Change the catalog and schema in the cells if you use different names, and swap the endpoint in section 6 for any chat model your workspace has.

On serverless, check the environment version in the notebook's Environment side panel before you run it, and set it explicitly when you run the notebook as a job. `ai_decide` returns a `VARIANT`, and the client in environment version 1 cannot read that type, so the first cell fails with `[UNSUPPORTED_OPERATION] data type is not supported`. Version 2 or above works.

Expect your numbers to differ from the article's. Answers are not deterministic: labels tend to hold, but scores and probabilities move from call to call, which is why the grades are materialized once and the gate reads from the table. The labels table in section 10 starts empty; add your own reviewer labels before trusting any threshold. The cost cell can return nothing for a few hours after the first calls, because billing usage lags. The Cleanup cell's `DROP` statements are commented out so the outputs are left for inspection; uncomment them to remove everything.
