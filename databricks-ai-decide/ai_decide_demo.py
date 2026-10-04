# Databricks notebook source
# MAGIC %md
# MAGIC # ai_decide(): the AI judge that never writes a word
# MAGIC
# MAGIC **Article:** [Databricks ai_decide: The AI Judge That Never Writes a Word](https://medium.com/@cralle/38139e614702?sk=28175398b6b8ad592de4f249159fa4b1)
# MAGIC
# MAGIC `ai_decide()` is a Databricks AI Function powered by a decision model. Instead of
# MAGIC generating text, it answers typed questions about a piece of text or JSON and
# MAGIC returns probabilities, choices, and scores. Three question types are supported:
# MAGIC `noul` (yes or no, as a probability), `choice`, and `score`.
# MAGIC
# MAGIC This notebook has two parts, using a CH Enterprise support inbox:
# MAGIC 1. **Sections 1 to 5:** learn the output on a simple triage pass, and see what the
# MAGIC    numbers actually look like (including how much they move between runs).
# MAGIC 2. **Sections 6 to 11:** the real job. `ai_query` drafts a reply to every ticket,
# MAGIC    `ai_decide` grades every draft, and a gate decides send, rewrite, or hold.
# MAGIC
# MAGIC **Requirements**
# MAGIC - Access to the `ai_decide` Beta (workspace admins control it on the Previews page)
# MAGIC - Databricks Runtime 15.4 LTS or above (18.2 or above recommended)
# MAGIC - On serverless, environment version 2 or above (version 1 cannot read the
# MAGIC   `VARIANT` result)
# MAGIC - Not available on Classic SQL warehouses
# MAGIC - A region that supports AI Functions
# MAGIC - A Foundation Model API chat endpoint for the drafts (section 6 uses
# MAGIC   `databricks-claude-haiku-4-5`; swap in any chat endpoint your workspace has)
# MAGIC
# MAGIC Sample tables live in `testing.default`.

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Warm-up: one ticket, three questions
# MAGIC One call, three questions: which team owns the ticket (`choice`), is the
# MAGIC customer at risk of leaving (`noul`), and how urgent is it (`score`).
# MAGIC The result is a `VARIANT` with one entry per question under `response.answers`.

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT ai_decide(
# MAGIC   'Nobody on our team can log in since 8am and we have a client demo at 2pm. This is the third outage this month and we are looking at alternatives.',
# MAGIC   '{
# MAGIC     "category": {
# MAGIC       "type": "choice",
# MAGIC       "instructions": "Which team should handle this support ticket?",
# MAGIC       "criteria": {
# MAGIC         "outage": "The service is down or users cannot log in",
# MAGIC         "bug": "A feature works incorrectly or is very slow, but the service is up",
# MAGIC         "billing": "Invoices, payments, refunds, or pricing",
# MAGIC         "account": "Users, permissions, subscriptions, or cancellations",
# MAGIC         "how_to": "The customer asks how to do something in the product",
# MAGIC         "feature_request": "The customer asks for something the product does not do today",
# MAGIC         "other": "Feedback, thanks, or anything that fits no other category"
# MAGIC       }
# MAGIC     },
# MAGIC     "churn_risk": {
# MAGIC       "type": "noul",
# MAGIC       "instructions": "Is this customer at risk of leaving CH Enterprise?"
# MAGIC     },
# MAGIC     "urgency": {
# MAGIC       "type": "score",
# MAGIC       "instructions": "How urgent is this ticket for the customer?",
# MAGIC       "criteria": [
# MAGIC         "Can wait: no impact on daily work",
# MAGIC         "Minor: an inconvenience with an easy workaround",
# MAGIC         "Major: important work is blocked for some users",
# MAGIC         "Critical: the customer cannot operate or a deadline is at risk today"
# MAGIC       ]
# MAGIC     }
# MAGIC   }',
# MAGIC   map('version', '1.0')
# MAGIC ) AS decision;

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Create the sample support inbox
# MAGIC Seven CH Enterprise tickets covering an outage, billing, a feature request, a
# MAGIC how-to, a slow page, a cancellation, and a thank-you with a follow-up question
# MAGIC tucked in.

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE testing.default.support_tickets (
# MAGIC   ticket_id     INT,
# MAGIC   customer_tier STRING,
# MAGIC   subject       STRING,
# MAGIC   body          STRING
# MAGIC );
# MAGIC
# MAGIC INSERT INTO testing.default.support_tickets VALUES
# MAGIC   (1, 'enterprise', 'Cannot log in', 'Nobody on our team can log in since 8am and we have a client demo at 2pm. This is the third outage this month and we are looking at alternatives.'),
# MAGIC   (2, 'pro', 'Charged twice', 'We were charged twice for the October invoice. Please refund the duplicate payment.'),
# MAGIC   (3, 'free', 'Excel export', 'It would be great if the dashboard export supported Excel as well as CSV.'),
# MAGIC   (4, 'pro', 'Adding a user', 'How do I add a new user to our workspace? I could not find it in the settings.'),
# MAGIC   (5, 'enterprise', 'Slow reports', 'Since the last update the report page takes over a minute to load. It is usable but painful, and our renewal is next month.'),
# MAGIC   (6, 'pro', 'Cancel subscription', 'Please cancel our subscription at the end of the term. We have moved to another vendor.'),
# MAGIC   (7, 'enterprise', 'Thanks', 'Thanks for the quick fix yesterday, everything works again. One question though: will the same thing happen at month end?');
# MAGIC
# MAGIC SELECT * FROM testing.default.support_tickets ORDER BY ticket_id;

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Triage every ticket
# MAGIC The `state` is a small JSON document built with `to_json(named_struct(...))`, so
# MAGIC the customer tier travels with the text. The `questions` argument must be a
# MAGIC constant string; it cannot come from a column or a table. The `noul` question
# MAGIC here uses the optional `true` / `false` criteria to define what churn risk means.
# MAGIC
# MAGIC The colon operator wants a column, so the call sits in a CTE and the answers are
# MAGIC extracted with `decision:response.answers.<question_id>.<field>` and a cast.

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE testing.default.support_tickets_triaged AS
# MAGIC WITH decided AS (
# MAGIC   SELECT
# MAGIC     ticket_id,
# MAGIC     customer_tier,
# MAGIC     subject,
# MAGIC     body,
# MAGIC     ai_decide(
# MAGIC       to_json(named_struct('subject', subject, 'body', body, 'customer_tier', customer_tier)),
# MAGIC       '{
# MAGIC         "category": {
# MAGIC           "type": "choice",
# MAGIC           "instructions": "Which team should handle this support ticket?",
# MAGIC           "criteria": {
# MAGIC             "outage": "The service is down or users cannot log in",
# MAGIC             "bug": "A feature works incorrectly or is very slow, but the service is up",
# MAGIC             "billing": "Invoices, payments, refunds, or pricing",
# MAGIC             "account": "Users, permissions, subscriptions, or cancellations",
# MAGIC             "how_to": "The customer asks how to do something in the product",
# MAGIC             "feature_request": "The customer asks for something the product does not do today",
# MAGIC             "other": "Feedback, thanks, or anything that fits no other category"
# MAGIC           }
# MAGIC         },
# MAGIC         "churn_risk": {
# MAGIC           "type": "noul",
# MAGIC           "instructions": "Is this customer at risk of leaving CH Enterprise?",
# MAGIC           "criteria": {
# MAGIC             "true": "The customer mentions alternatives, canceling, repeated problems, or an upcoming renewal in a negative tone.",
# MAGIC             "false": "The customer is neutral or satisfied and gives no sign of leaving."
# MAGIC           }
# MAGIC         },
# MAGIC         "urgency": {
# MAGIC           "type": "score",
# MAGIC           "instructions": "How urgent is this ticket for the customer?",
# MAGIC           "criteria": [
# MAGIC             "Can wait: no impact on daily work",
# MAGIC             "Minor: an inconvenience with an easy workaround",
# MAGIC             "Major: important work is blocked for some users",
# MAGIC             "Critical: the customer cannot operate or a deadline is at risk today"
# MAGIC           ]
# MAGIC         }
# MAGIC       }',
# MAGIC       map('version', '1.0')
# MAGIC     ) AS decision
# MAGIC   FROM testing.default.support_tickets
# MAGIC )
# MAGIC SELECT
# MAGIC   ticket_id,
# MAGIC   customer_tier,
# MAGIC   subject,
# MAGIC   body,
# MAGIC   decision,
# MAGIC   decision:response.answers.category.choice::STRING         AS category,
# MAGIC   decision:response.answers.category.confidence::DOUBLE     AS category_confidence,
# MAGIC   decision:response.answers.churn_risk.probability::DOUBLE  AS churn_probability,
# MAGIC   decision:response.answers.urgency.score::DOUBLE           AS urgency_score,
# MAGIC   decision:response.answers.urgency.confidence::DOUBLE      AS urgency_confidence,
# MAGIC   decision:error_message::STRING                            AS error_message
# MAGIC FROM decided;
# MAGIC
# MAGIC SELECT
# MAGIC   ticket_id, subject, category, category_confidence,
# MAGIC   churn_probability, urgency_score, error_message
# MAGIC FROM testing.default.support_tickets_triaged
# MAGIC ORDER BY ticket_id;

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Look inside a choice answer
# MAGIC The `probabilities` object on a `choice` answer holds a probability for every
# MAGIC label. This shows it for the ticket with the lowest category confidence; answers
# MAGIC can shift between runs, so the query picks that ticket instead of hardcoding an
# MAGIC ID. `variant_explode` turns the object into one row per label.
# MAGIC
# MAGIC In most test runs for this notebook, the label probabilities came back
# MAGIC all-or-nothing even when `confidence` was below 1. When they do spread, they name
# MAGIC the runner-up; `confidence` is the first signal to check.

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC   t.ticket_id,
# MAGIC   t.subject,
# MAGIC   e.key                     AS label,
# MAGIC   ROUND(e.value::DOUBLE, 3) AS probability
# MAGIC FROM testing.default.support_tickets_triaged AS t,
# MAGIC LATERAL variant_explode(t.decision:response.answers.category.probabilities) AS e
# MAGIC WHERE t.ticket_id = (
# MAGIC   SELECT ticket_id
# MAGIC   FROM testing.default.support_tickets_triaged
# MAGIC   ORDER BY category_confidence ASC, ticket_id
# MAGIC   LIMIT 1
# MAGIC )
# MAGIC ORDER BY probability DESC;

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5. Same ticket, three calls
# MAGIC `ai_decide` is not deterministic. This scores the urgency of every ticket three
# MAGIC times in one query. Labels tend to hold; scores and probabilities move, and they
# MAGIC come back in coarse steps. Both facts shape how you set thresholds later.

# COMMAND ----------

# MAGIC %sql
# MAGIC WITH runs AS (
# MAGIC   SELECT
# MAGIC     t.ticket_id,
# MAGIC     t.subject,
# MAGIC     r.id AS run,
# MAGIC     ai_decide(
# MAGIC       to_json(named_struct('subject', t.subject, 'body', t.body, 'customer_tier', t.customer_tier)),
# MAGIC       '{
# MAGIC         "urgency": {
# MAGIC           "type": "score",
# MAGIC           "instructions": "How urgent is this ticket for the customer?",
# MAGIC           "criteria": [
# MAGIC             "Can wait: no impact on daily work",
# MAGIC             "Minor: an inconvenience with an easy workaround",
# MAGIC             "Major: important work is blocked for some users",
# MAGIC             "Critical: the customer cannot operate or a deadline is at risk today"
# MAGIC           ]
# MAGIC         }
# MAGIC       }',
# MAGIC       map('version', '1.0')
# MAGIC     ) AS d
# MAGIC   FROM testing.default.support_tickets AS t
# MAGIC   CROSS JOIN range(3) AS r
# MAGIC ),
# MAGIC scored AS (
# MAGIC   SELECT ticket_id, subject, run, d:response.answers.urgency.score::DOUBLE AS urgency
# MAGIC   FROM runs
# MAGIC )
# MAGIC SELECT
# MAGIC   ticket_id,
# MAGIC   subject,
# MAGIC   array_sort(collect_list(ROUND(urgency, 2))) AS urgency_by_run,
# MAGIC   ROUND(MAX(urgency) - MIN(urgency), 2)       AS spread
# MAGIC FROM scored
# MAGIC GROUP BY ticket_id, subject
# MAGIC ORDER BY ticket_id;

# COMMAND ----------

# MAGIC %md
# MAGIC ## 6. Draft a reply to every ticket with ai_query
# MAGIC Part two starts here. CH Enterprise wants an LLM to draft replies to support
# MAGIC tickets. The prompt is deliberately plain, the kind that ends up in a first
# MAGIC version. The endpoint name must be a constant; swap it for any chat model in your
# MAGIC workspace.

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE testing.default.support_reply_drafts AS
# MAGIC SELECT
# MAGIC   ticket_id,
# MAGIC   subject,
# MAGIC   body,
# MAGIC   ai_query(
# MAGIC     'databricks-claude-haiku-4-5',
# MAGIC     concat('Write a short, friendly reply to this CH Enterprise support ticket:\n\n', body)
# MAGIC   ) AS draft_reply
# MAGIC FROM testing.default.support_tickets;
# MAGIC
# MAGIC SELECT ticket_id, subject, draft_reply
# MAGIC FROM testing.default.support_reply_drafts
# MAGIC ORDER BY ticket_id;

# COMMAND ----------

# MAGIC %md
# MAGIC ## 7. Grade every draft with ai_decide
# MAGIC The state holds both the ticket and the reply, so the judge sees the question and
# MAGIC the answer together. Three short questions:
# MAGIC - `answers_question` (`noul`): does the reply answer what the customer asked?
# MAGIC - `makes_promise` (`noul`): does it promise a refund, a credit, or a date?
# MAGIC - `quality` (`score`, 0 to 2): poor, okay, or good.
# MAGIC
# MAGIC The grades are materialized once. `ai_decide` is not deterministic, so decide from
# MAGIC the table rather than calling the function again on every read. Each row also
# MAGIC records the judge version and the `ai_decide` version.

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE testing.default.support_reply_evals AS
# MAGIC WITH judged AS (
# MAGIC   SELECT
# MAGIC     ticket_id,
# MAGIC     subject,
# MAGIC     draft_reply,
# MAGIC     ai_decide(
# MAGIC       to_json(named_struct('ticket', body, 'reply', draft_reply)),
# MAGIC       '{
# MAGIC         "answers_question": {"type": "noul", "instructions": "Does the reply answer what the customer asked?"},
# MAGIC         "makes_promise":    {"type": "noul", "instructions": "Does the reply promise a refund, a credit, or a date?"},
# MAGIC         "quality": {
# MAGIC           "type": "score",
# MAGIC           "instructions": "How good is this reply?",
# MAGIC           "criteria": [
# MAGIC             "Poor: the customer would have to write again",
# MAGIC             "Okay: it answers part of the ticket",
# MAGIC             "Good: a clear answer and a next step"
# MAGIC           ]
# MAGIC         }
# MAGIC       }',
# MAGIC       map('version', '1.0')
# MAGIC     ) AS j
# MAGIC   FROM testing.default.support_reply_drafts
# MAGIC )
# MAGIC SELECT
# MAGIC   ticket_id,
# MAGIC   subject,
# MAGIC   draft_reply,
# MAGIC   j,
# MAGIC   j:response.answers.answers_question.probability::DOUBLE AS p_answers,
# MAGIC   j:response.answers.makes_promise.probability::DOUBLE    AS p_promise,
# MAGIC   j:response.answers.quality.score::DOUBLE                AS quality,
# MAGIC   j:error_message::STRING                                 AS error_message,
# MAGIC   j:metadata.version::STRING                              AS ai_decide_version,
# MAGIC   'reply-judge-v1'                                        AS judge_version,
# MAGIC   current_timestamp()                                     AS judged_at
# MAGIC FROM judged;

# COMMAND ----------

# MAGIC %md
# MAGIC ## 8. Gate each draft: send, rewrite, or hold
# MAGIC First match wins, so the `WHEN`s are ordered by the cost of a miss: a promised
# MAGIC refund is worse than a vague reply. Values mostly come back in steps of 0.05, so
# MAGIC the thresholds sit between the steps (0.33, not 0.35). A line on the grid lets
# MAGIC `>=` versus `>` decide the row. Treat them as starting points and calibrate them
# MAGIC in section 10.

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE VIEW testing.default.support_reply_gate AS
# MAGIC SELECT
# MAGIC   ticket_id,
# MAGIC   subject,
# MAGIC   draft_reply,
# MAGIC   ROUND(p_answers, 2) AS p_answers,
# MAGIC   ROUND(p_promise, 2) AS p_promise,
# MAGIC   ROUND(quality, 2)   AS quality,
# MAGIC   CASE
# MAGIC     WHEN error_message IS NOT NULL OR p_answers IS NULL THEN 'retry'
# MAGIC     WHEN p_promise >= 0.33                             THEN 'hold'
# MAGIC     WHEN p_answers < 0.63 OR quality < 1.23            THEN 'rewrite'
# MAGIC     ELSE 'send'
# MAGIC   END AS gate
# MAGIC FROM testing.default.support_reply_evals;
# MAGIC
# MAGIC SELECT * FROM testing.default.support_reply_gate ORDER BY ticket_id;

# COMMAND ----------

# MAGIC %md
# MAGIC ## 9. Count the gates
# MAGIC One number per gate. Track it on every run: if the share going to `rewrite` or
# MAGIC `hold` jumps after someone changes the prompt or the model, you have caught a
# MAGIC regression without reading a single draft.

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT gate, COUNT(*) AS drafts
# MAGIC FROM testing.default.support_reply_gate
# MAGIC GROUP BY gate
# MAGIC ORDER BY drafts DESC;

# COMMAND ----------

# MAGIC %md
# MAGIC ## 10. Calibrate the thresholds against human labels
# MAGIC A reviewer reads a sample of drafts and marks each one acceptable or not. Join the
# MAGIC labels to the grades and look at the acceptance rate per probability bucket; draw
# MAGIC the line where the rate is high enough for what `send` triggers. Seven tickets is
# MAGIC far too few for this, so the labels table starts empty: add a few hundred labeled
# MAGIC drafts from your own data before trusting any threshold.

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE IF NOT EXISTS testing.default.support_reply_labels (
# MAGIC   ticket_id   INT,
# MAGIC   reviewer_ok BOOLEAN
# MAGIC );
# MAGIC
# MAGIC -- Example of adding labels after reading the drafts:
# MAGIC -- INSERT INTO testing.default.support_reply_labels VALUES (1, false), (2, true);
# MAGIC
# MAGIC SELECT
# MAGIC   FLOOR(e.p_answers * 5) / 5                                   AS p_answers_bucket,
# MAGIC   COUNT(*)                                                     AS labeled_drafts,
# MAGIC   ROUND(AVG(CASE WHEN l.reviewer_ok THEN 1.0 ELSE 0.0 END), 2) AS reviewer_ok_rate
# MAGIC FROM testing.default.support_reply_evals AS e
# MAGIC JOIN testing.default.support_reply_labels AS l
# MAGIC   USING (ticket_id)
# MAGIC GROUP BY 1
# MAGIC ORDER BY 1;

# COMMAND ----------

# MAGIC %md
# MAGIC ## 11. Check for failures
# MAGIC On failure, `response` is null and `error_message` describes the problem. A bad
# MAGIC row does not fail the query, so it will not fail loudly either. Check every run.

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT 'triage' AS step, COUNT(*) AS calls, COUNT_IF(error_message IS NOT NULL) AS failed
# MAGIC FROM testing.default.support_tickets_triaged
# MAGIC UNION ALL
# MAGIC SELECT 'reply judge', COUNT(*), COUNT_IF(error_message IS NOT NULL OR p_answers IS NULL)
# MAGIC FROM testing.default.support_reply_evals;

# COMMAND ----------

# MAGIC %md
# MAGIC ## 12. The same judge in a DataFrame pipeline
# MAGIC Keep the questions as a Python dict and serialize them once. The JSON ends up
# MAGIC inside a single-quoted SQL literal, so backslashes and apostrophes are escaped.
# MAGIC The hash gives every version of the questions a stable ID to store next to the
# MAGIC grades.
# MAGIC
# MAGIC This grades the same drafts a second time, so compare it with section 8: some
# MAGIC verdicts will change, which is why the grades are materialized once.

# COMMAND ----------

import hashlib
import json

import pyspark.sql.functions as F

JUDGE_QUESTIONS = {
    "answers_question": {"type": "noul", "instructions": "Does the reply answer what the customer asked?"},
    "makes_promise": {"type": "noul", "instructions": "Does the reply promise a refund, a credit, or a date?"},
    "quality": {
        "type": "score",
        "instructions": "How good is this reply?",
        "criteria": [
            "Poor: the customer would have to write again",
            "Okay: it answers part of the ticket",
            "Good: a clear answer and a next step",
        ],
    },
}

JUDGE_JSON = json.dumps(JUDGE_QUESTIONS)
JUDGE_SQL = JUDGE_JSON.replace("\\", "\\\\").replace("'", "\\'")
JUDGE_HASH = hashlib.sha256(JUDGE_JSON.encode()).hexdigest()[:12]

graded_df = (
    spark.read.table("testing.default.support_reply_drafts")
    .withColumn("state", F.to_json(F.struct(F.col("body").alias("ticket"), F.col("draft_reply").alias("reply"))))
    .withColumn("j", F.expr(f"ai_decide(state, '{JUDGE_SQL}', map('version', '1.0'))"))
    .select(
        "ticket_id",
        F.expr("j:response.answers.answers_question.probability::DOUBLE").alias("p_answers"),
        F.expr("j:response.answers.makes_promise.probability::DOUBLE").alias("p_promise"),
        F.expr("j:response.answers.quality.score::DOUBLE").alias("quality"),
        F.lit(JUDGE_HASH).alias("judge_hash"),
    )
)

display(graded_df.orderBy("ticket_id"))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 13. Optional: the REST API, and measure the latency yourself
# MAGIC The same function is exposed at `POST /api/2.0/ai-functions/ai-decide` with the
# MAGIC same `state`, `questions`, and `options`. Here `questions` is a JSON object, not
# MAGIC a string. On success the response has `response` and `metadata` but no
# MAGIC `error_message` key, so check for `response` rather than for a null error.
# MAGIC
# MAGIC Databricks publishes no latency numbers, so this cell times ten calls from the
# MAGIC notebook. The figure includes the network round trip from the notebook to the API.

# COMMAND ----------

import statistics
import time

from databricks.sdk import WorkspaceClient

w = WorkspaceClient()

sample = spark.read.table("testing.default.support_reply_drafts").orderBy("ticket_id").first()
body = {
    "state": {"ticket": sample["body"], "reply": sample["draft_reply"]},
    "questions": JUDGE_QUESTIONS,
    "options": {"version": "1.0"},
}

latencies_ms = []
for _ in range(10):
    start = time.perf_counter()
    response = w.api_client.do("POST", "/api/2.0/ai-functions/ai-decide", body=body)
    latencies_ms.append((time.perf_counter() - start) * 1000)

print(json.dumps(response, indent=2))
print(f"p50 {statistics.median(latencies_ms):.0f} ms, min {min(latencies_ms):.0f} ms, max {max(latencies_ms):.0f} ms over {len(latencies_ms)} calls")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 14. Optional: see what it cost
# MAGIC AI Functions are billed on top of query compute. Rather than assume which product
# MAGIC and feature name `ai_decide` bills under, list the AI Function usage in the last
# MAGIC week and find it there. Usage data can lag by several hours.

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC   usage_date,
# MAGIC   billing_origin_product,
# MAGIC   product_features.ai_functions.ai_function AS ai_function,
# MAGIC   SUM(usage_quantity)                        AS usage_quantity
# MAGIC FROM system.billing.usage
# MAGIC WHERE usage_date >= current_date() - INTERVAL 7 DAYS
# MAGIC   AND product_features.ai_functions.ai_function IS NOT NULL
# MAGIC GROUP BY ALL
# MAGIC ORDER BY usage_date DESC, usage_quantity DESC;

# COMMAND ----------

# MAGIC %md
# MAGIC ## Cleanup (optional)

# COMMAND ----------

# MAGIC %sql
# MAGIC -- DROP VIEW IF EXISTS testing.default.support_reply_gate;
# MAGIC -- DROP TABLE IF EXISTS testing.default.support_reply_labels;
# MAGIC -- DROP TABLE IF EXISTS testing.default.support_reply_evals;
# MAGIC -- DROP TABLE IF EXISTS testing.default.support_reply_drafts;
# MAGIC -- DROP TABLE IF EXISTS testing.default.reply_prompt_versions;
# MAGIC -- DROP TABLE IF EXISTS testing.default.support_tickets_triaged;
# MAGIC -- DROP TABLE IF EXISTS testing.default.support_tickets;

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC **Notes**
# MAGIC - `ai_decide` is in Beta. Pin `map('version', '1.0')` in every call so the output
# MAGIC   contract does not shift under you.
# MAGIC - Question types: `noul` (probability from 0 to 1, optional `true` / `false`
# MAGIC   criteria), `choice` (1 to 255 labels), and `score` (2 to 10 ordered levels,
# MAGIC   returned as a probability-weighted average of the level indices).
# MAGIC - `confidence` on `choice` and `score` answers is documented as how well the state
# MAGIC   supports the assessment. It is not the top probability. In most test runs for
# MAGIC   this notebook, label probabilities came back all-or-nothing.
# MAGIC - The `questions` argument must be constant. Nonconstant question definitions
# MAGIC   cause query errors, so keep them in code and version them with the pipeline.
# MAGIC - Answers are not deterministic, and values mostly come back in steps of 0.05.
# MAGIC   Materialize the grades once, put thresholds between the steps, and judge
# MAGIC   thresholds against labeled data rather than a single run.
# MAGIC - The judge only checks what you ask. A reply that invents a fact passes unless a
# MAGIC   question asks about unsupported claims.
# MAGIC - On failure, `response` is null and `error_message` describes the failure.
# MAGIC - The function returns a `VARIANT`. On serverless environment version 1 the
# MAGIC   notebook client cannot read that type and the cell fails with
# MAGIC   `[UNSUPPORTED_OPERATION] data type is not supported`; use version 2 or above.
# MAGIC - The underlying models are Apache 2.0 licensed. Data is processed within the
# MAGIC   Databricks security perimeter; parameters are not stored, only metadata.
# MAGIC - `ai_decide` is directly compatible with the TypeSafe AI API.
# MAGIC
# MAGIC Sources:
# MAGIC - ai_decide function docs, Databricks
# MAGIC   (https://docs.databricks.com/aws/en/sql/language-manual/functions/ai_decide)
# MAGIC - AI Functions REST API, Databricks
# MAGIC   (https://docs.databricks.com/api/ai-functions/v1)
# MAGIC - Enrich data using AI Functions, Databricks
# MAGIC   (https://docs.databricks.com/aws/en/large-language-models/ai-functions)
# MAGIC - ai_query function docs, Databricks
# MAGIC   (https://docs.databricks.com/aws/en/sql/language-manual/functions/ai_query)
# MAGIC - Introducing ai_decide, Databricks blog
# MAGIC   (https://www.databricks.com/blog/introducing-aidecide-make-fast-decisions-your-governed-data)
