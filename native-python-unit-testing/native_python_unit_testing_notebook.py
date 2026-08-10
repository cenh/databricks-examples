# Databricks notebook source
# MAGIC %md
# MAGIC # Unit Testing in Databricks Notebooks: Native pytest Support in the Workspace
# MAGIC
# MAGIC **Author:** Christian Hansen ([https://medium.com/@cralle](https://medium.com/@cralle))
# MAGIC
# MAGIC **Published:** March 2, 2026
# MAGIC
# MAGIC Databricks now supports native Python unit testing directly inside the Workspace: open a `test_*.py` file, and a Tests sidebar panel, inline run buttons, inline failure indicators, and a dedicated Testing tab all appear automatically, using standard pytest conventions, no external test runner or CI step required.
# MAGIC
# MAGIC **Article:** [Unit Testing in Databricks Notebooks: Native pytest Support in the Workspace](https://medium.com/@cralle/native-python-unit-testing-in-databricks-notebooks-29e3130a36ec?sk=5ffec8e03832fc6f108ed35620d6cd7d)
# MAGIC
# MAGIC **Requires:** a Databricks Workspace with an attached, running cluster. The Tests sidebar panel, inline run buttons, inline failure indicators, and the Testing tab are Workspace UI features, they only appear when you open an actual `.py` file (not a notebook cell) in the Workspace file editor, so those parts are described inline rather than executed. The Setup cell below writes real `maths.py` and `test_maths.py` files into this notebook's own Workspace folder so you can open them yourself and see the UI behavior described in this notebook.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Setup
# MAGIC
# MAGIC This article is about pytest-based unit testing of plain Python files stored in the Databricks Workspace, not about notebook cells. To make that concrete, the cells below:
# MAGIC
# MAGIC 1. Make sure `pytest` is available on the cluster's Python environment.
# MAGIC 2. Resolve this notebook's own Workspace folder, so the example files land next to it.
# MAGIC 3. Write two real files into that folder: `maths.py` (a small function) and `test_maths.py` (a pytest test file that imports it).
# MAGIC
# MAGIC Once those files exist, opening `test_maths.py` in the Workspace file editor is what triggers the Tests sidebar panel, inline run buttons, and the Testing tab described further down. That part of the workflow is a UI interaction and cannot be triggered from notebook code, see the **Manual setup required** section in the companion README for the click-by-click steps.

# COMMAND ----------

# MAGIC %pip install pytest --quiet

# COMMAND ----------

import os
import sys

# Resolve this notebook's own folder in the Workspace, so the example files
# are written right next to it and can be opened from the Workspace file browser.
notebook_path = dbutils.notebook.entry_point.getDbutils().notebook().getContext().notebookPath().get()
workspace_dir = "/Workspace" + notebook_path.rsplit("/", 1)[0]

os.makedirs(workspace_dir, exist_ok=True)
os.chdir(workspace_dir)
if workspace_dir not in sys.path:
    sys.path.insert(0, workspace_dir)

print(f"Example files will be written to: {workspace_dir}")

# COMMAND ----------

# MAGIC %md
# MAGIC The two files below mirror what CH Enterprise's data engineering team would put in a real Workspace project: a small reusable module (`maths.py`) plus a pytest file that imports and tests it (`test_maths.py`), instead of writing tests inline in a notebook cell.

# COMMAND ----------

maths_py_source = '''"""Small arithmetic helper module, used as a stand-in for a CH Enterprise
shared library function that other Workspace files can import and test."""


def sum_numbers(a, b):
    return a + b
'''

# This assertion is written to intentionally fail: sum_numbers(2, 3) is 5, not 6.
# It mirrors the real product screenshots later in the article (Sidebar Test Panel
# and Dedicated Test Results Panel), which both show this exact test failing on
# `assert sum_numbers(2, 3) == 6`.
test_maths_py_source = '''from maths import sum_numbers


def test_sum_positive_numbers():
    assert sum_numbers(2, 3) == 6
'''

with open(os.path.join(workspace_dir, "maths.py"), "w") as f:
    f.write(maths_py_source)

with open(os.path.join(workspace_dir, "test_maths.py"), "w") as f:
    f.write(test_maths_py_source)

print("Wrote maths.py and test_maths.py to:", workspace_dir)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Test Discovery Follows pytest Conventions
# MAGIC
# MAGIC Databricks follows standard pytest conventions for discovering tests in the Workspace:
# MAGIC
# MAGIC **Valid test files**
# MAGIC - `test_*.py`
# MAGIC - `*_test.py`
# MAGIC
# MAGIC **Valid test functions**
# MAGIC - `test_*`-prefixed functions or methods outside a class
# MAGIC - `test_*`-prefixed methods inside `Test*`-prefixed classes that have no `__init__` method
# MAGIC - Methods decorated with `@staticmethod` or `@classmethod` inside `Test*`-prefixed classes
# MAGIC
# MAGIC Example (this is illustrative code, shown here as a plain code block, not written to a file, unlike `maths.py` and `test_maths.py` above):
# MAGIC
# MAGIC ```python
# MAGIC def sum_numbers(a, b):
# MAGIC     return a + b
# MAGIC
# MAGIC def test_sum_positive_numbers():
# MAGIC     assert sum_numbers(2, 3) == 6
# MAGIC ```
# MAGIC
# MAGIC Open a valid test file in the Workspace file editor and Databricks automatically detects your tests, no configuration required.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Using Code From Other Files and Folders
# MAGIC
# MAGIC Unit tests are not limited to code in the same file. You can import functions and classes from other modules in your Workspace and test them directly, which is exactly what the `maths.py` and `test_maths.py` files written during Setup do:
# MAGIC
# MAGIC `maths.py`:
# MAGIC
# MAGIC ```python
# MAGIC def sum_numbers(a, b):
# MAGIC     return a + b
# MAGIC ```
# MAGIC
# MAGIC `test_maths.py`:
# MAGIC
# MAGIC ```python
# MAGIC from maths import sum_numbers
# MAGIC
# MAGIC def test_sum_positive_numbers():
# MAGIC     assert sum_numbers(2, 3) == 6
# MAGIC ```
# MAGIC
# MAGIC This particular assertion is deliberately wrong (2 + 3 is 5, not 6), it is the same `test_sum_positive_numbers` that later shows up failing in the Sidebar Test Panel and the Dedicated Test Results Panel below, so keep an eye on it as you read on.
# MAGIC
# MAGIC This pattern lets you structure Workspace projects properly: reusable modules, a clean separation between logic and tests, and real project-style layouts, all inside Databricks.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Sidebar Test Panel
# MAGIC
# MAGIC When a test file is open, a **Tests panel** appears automatically in the sidebar. Inside an authoring context, a notebook or file editor session with an attached cluster, discovery is not limited to the file you have open: it covers every test file in that context. From here you can:
# MAGIC
# MAGIC - Run all tests
# MAGIC - Run only failed tests
# MAGIC - Run individual tests
# MAGIC - Filter by name or status
# MAGIC - See pass/fail status live
# MAGIC
# MAGIC With `test_maths.py` open (as written during Setup), the Tests panel shows a summary such as "3 passed / 1 failed", with `test_maths.py` expanded and `test_sum_positive_numbers` marked as failed, which matches the deliberately wrong `== 6` assertion above. This gives you full test control without leaving the Workspace UI. This panel is a Workspace UI feature and only appears once you open `test_maths.py` as a file, see **Manual setup required** in the README.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Inline Test Execution
# MAGIC
# MAGIC Each test also gets inline run buttons directly in the file editor. You can run:
# MAGIC
# MAGIC - A single test
# MAGIC - A class of tests
# MAGIC - The full file
# MAGIC
# MAGIC Results update immediately next to the test definition. Like the sidebar panel, these inline run buttons only appear in the Workspace file editor, not in a notebook cell.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Inline Failure Debugging
# MAGIC
# MAGIC When a test fails:
# MAGIC
# MAGIC - The failing line is highlighted
# MAGIC - A failure indicator appears inline
# MAGIC - Clicking it shows the full error message
# MAGIC
# MAGIC For `test_maths.py`, that means the editor highlights `assert sum_numbers(2, 3) == 6` on line 4 and lets you click straight through to the failure detail, no log hunting, no stack trace digging.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Dedicated Test Results Panel
# MAGIC
# MAGIC A **Testing** tab appears in the bottom pane showing:
# MAGIC
# MAGIC - Full test summary
# MAGIC - Individual test results
# MAGIC - Navigation to test code
# MAGIC - Re-run options
# MAGIC
# MAGIC For the files written in Setup, this tab shows the traceback for `test_sum_positive_numbers`: `assert sum_numbers(2, 3) == 6`, `AssertionError`, `test_maths.py:4: AssertionError`. It behaves like a lightweight, built-in test runner UI.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Running the Same Test Logic Without the UI
# MAGIC
# MAGIC The Tests panel, inline run buttons, and Testing tab above are all Workspace UI features. To show that the underlying test is genuinely runnable end to end, without any of that UI, the two cells below run the exact same `test_maths.py` against the exact same `maths.py` written in Setup: first with `pytest` itself via a shell cell, then with a plain Python assertion. Both are expected to fail, on purpose, exactly like the screenshots described above.

# COMMAND ----------

os.environ["WORKSPACE_DIR"] = workspace_dir

# COMMAND ----------

# MAGIC %sh
# MAGIC cd "$WORKSPACE_DIR"
# MAGIC pytest test_maths.py -v || true

# COMMAND ----------

from maths import sum_numbers


def test_sum_positive_numbers():
    assert sum_numbers(2, 3) == 6


try:
    test_sum_positive_numbers()
    print("test_sum_positive_numbers: PASSED (unexpected)")
except AssertionError as e:
    print(f"test_sum_positive_numbers: FAILED as expected -> {e}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Why This Matters
# MAGIC
# MAGIC This is more than a UI feature, it changes how notebooks and Workspace files are used:
# MAGIC
# MAGIC - Testing becomes part of daily development
# MAGIC - Faster feedback loops
# MAGIC - Less context switching
# MAGIC - Better engineering discipline in data workflows
# MAGIC - Notebooks and Workspace files become real dev environments, not just scratchpads
# MAGIC
# MAGIC For teams building production-grade data platforms, this is a big step toward treating notebooks and Workspace files as first-class engineering artifacts, not just interactive scripts.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Cleanup
# MAGIC
# MAGIC Removes the `maths.py` and `test_maths.py` files that Setup wrote into this notebook's Workspace folder.

# COMMAND ----------

for file_name in ("maths.py", "test_maths.py"):
    file_path = os.path.join(workspace_dir, file_name)
    if os.path.exists(file_path):
        os.remove(file_path)

print("Cleanup complete: maths.py and test_maths.py removed from", workspace_dir)

# COMMAND ----------

# MAGIC %md
# MAGIC **Source:**
# MAGIC - [Python unit testing in the workspace (Databricks on AWS)](https://docs.databricks.com/aws/en/files/python-unit-tests)
# MAGIC - [Python unit testing in the workspace (Microsoft Learn)](https://learn.microsoft.com/en-us/azure/databricks/files/python-unit-tests)
# MAGIC - [Unit testing for Databricks notebooks](https://docs.databricks.com/aws/en/notebooks/testing)
# MAGIC
# MAGIC **Notes:**
# MAGIC - `test_maths.py`'s `test_sum_positive_numbers` asserts `sum_numbers(2, 3) == 6`, which is deliberately wrong (2 + 3 is 5). This matches the real product screenshots in the article (Sidebar Test Panel and Dedicated Test Results Panel), which both show this exact test failing on the `== 6` assertion, so this notebook keeps it as `== 6` rather than "fixing" it to `== 5`.
# MAGIC - The Tests sidebar panel, inline run buttons, inline failure indicators, and Testing tab are Workspace UI features tied to opening an actual `.py` file in the Workspace file editor with an attached cluster. They cannot be triggered from notebook code, this notebook writes the real `maths.py` and `test_maths.py` files so you can open them yourself and see that behavior; see the companion README's "Manual setup required" section.
# MAGIC - The `%sh pytest` and plain Python assertion cells above run the same `test_sum_positive_numbers` test logic directly, without any Workspace UI, so at least part of this workflow is genuinely runnable end to end inside the notebook. Both are expected to fail, on purpose.
# MAGIC - This notebook does not use Unity Catalog tables, so `testing.default` is not referenced in code; sample code uses the fictional company CH Enterprise for consistency with the rest of this article series.
