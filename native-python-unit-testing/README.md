**Article:** [Unit Testing in Databricks Notebooks: Native pytest Support in the Workspace](https://medium.com/@cralle/native-python-unit-testing-in-databricks-notebooks-29e3130a36ec?sk=5ffec8e03832fc6f108ed35620d6cd7d)

# Unit Testing in Databricks Notebooks: Native pytest Support in the Workspace

Databricks now supports native Python unit testing directly inside the Workspace: open a `test_*.py` file and a Tests sidebar panel, inline run buttons, inline failure indicators, and a dedicated Testing tab all appear automatically, following standard pytest conventions. No external test runner, no CI step, and no leaving the Workspace UI.

The notebook writes a small `maths.py` module and a `test_maths.py` pytest file into its own Workspace folder, walks through how Databricks discovers and runs those tests through the Tests panel, inline run buttons, and Testing tab, then runs the exact same test logic directly inside the notebook (via `%sh pytest` and a plain Python assertion) so part of the workflow is genuinely runnable end to end without the UI, and cleans up the files it wrote.

## Files

- `native_python_unit_testing_notebook.py` - Databricks notebook (Python) covering the full pattern: setup, test discovery conventions, importing code from other Workspace files, the Sidebar Test Panel, inline execution and failure debugging, the Dedicated Test Results Panel, running the same test without the UI, and cleanup.

## Requirements

- A Databricks Workspace with an attached, running cluster (any current Databricks Runtime; pytest support in the Workspace is a platform feature, not tied to a specific runtime version)
- Ability to install a package on the cluster's Python environment (the notebook runs `%pip install pytest`, though most Databricks Runtimes already ship with pytest)
- No Unity Catalog privileges are required; this article does not create or read any tables

## Setup

Run the Setup cells first. They:

1. Install `pytest` on the cluster's Python environment.
2. Resolve this notebook's own folder in the Workspace.
3. Write two real files into that folder: `maths.py` (a small `sum_numbers` function) and `test_maths.py` (a pytest test file that imports `sum_numbers` and asserts `sum_numbers(2, 3) == 6`, deliberately wrong on purpose, see Notes below).

## Manual setup required

The core feature this article describes, the Tests sidebar panel, inline run buttons, inline failure indicators, and the Dedicated Test Results (Testing) panel, is a Databricks Workspace UI feature. It only activates when you open an actual `.py` file in the Workspace file editor with an attached cluster; it is not triggered by any notebook cell or API call. To see it yourself after running the Setup cells:

1. In the Workspace file browser, navigate to the same folder as this notebook (the Setup cell prints the exact path).
2. Confirm `pytest` is available on the cluster. Most current Databricks Runtimes ship with it already; if not, run `%pip install pytest` in a notebook cell attached to that cluster, as this notebook does.
3. Open `test_maths.py` as a file (double-click it in the Workspace browser, do not just view it inline in the notebook). Opening it as a file is what makes the Tests panel appear in the sidebar.
4. With `test_maths.py` open, look for:
   - The **Tests** panel in the sidebar, which lists discovered tests and lets you run all tests, only failed tests, or individual tests, and filter by name or status.
   - Inline run buttons next to `test_sum_positive_numbers` in the editor, letting you run that single test, a class of tests, or the whole file.
   - An inline failure indicator on the `assert sum_numbers(2, 3) == 6` line once you run the test (it fails, since 2 + 3 is 5, not 6); clicking it shows the full error.
   - A **Testing** tab in the bottom pane with the full test summary, individual results, navigation back to the test code, and re-run options.
5. When you are done, either leave the files for further experimentation or run the Cleanup cell in the notebook to remove them.

None of this UI behavior can be captured by running the notebook end to end; it requires the interactive Workspace file editor described above.

## Cleanup

Run the Cleanup cell at the end of the notebook to remove the `maths.py` and `test_maths.py` files that Setup wrote into this notebook's Workspace folder.
