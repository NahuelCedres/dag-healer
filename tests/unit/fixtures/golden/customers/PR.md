# dag-healer fix: `transform` in `customers_sync`

> Automated fix proposed by **dag-healer** for incident `00000000000000000000000000000002`. Nothing is merged automatically: review the change, the failing test and the test results below before merging.

## Summary

| Field | Value |
|---|---|
| DAG | `customers_sync` |
| Task | `transform` |
| Run | `manual__2026-10-18T09:00:00+00:00` |
| Try | 1 |
| Classification | CODE (confidence 0.90) |
| Incident | `00000000000000000000000000000002` |
| Branch | `healer/customers_sync-transform-00000000` |
| Change | 2 file(s), +19 / -2 lines |

Failure reported by Airflow (masked):

```text
ValueError: unconverted data remains: T08:30:00Z

Traceback (most recent call last):
  File "/home/airflow/.local/lib/python3.13/site-packages/airflow/sdk/execution_time/task_runner.py", line 1579, in _run_task_and_map_outcome
  File "/home/airflow/.local/lib/python3.13/site-packages/airflow/sdk/execution_time/task_runner.py", line 197, in wrapper
  File "/home/airflow/.local/lib/python3.13/site-packages/airflow/sdk/execution_time/task_runner.py", line 2234, in _execute_task
  File "/home/airflow/.local/lib/python3.13/site-packages/airflow/sdk/execution_time/task_runner.py", line 197, in wrapper
  File "/home/airflow/.local/lib/python3.13/site-packages/airflow/sdk/execution_time/task_runner.py", line 2192, in _run_execute_callable
  File "/home/airflow/.local/lib/python3.13/site-packages/airflow/sdk/bases/operator.py", line 462, in wrapper
  File "/home/airflow/.local/lib/python3.13/site-packages/airflow/providers/standard/operators/python.py", line 231, in execute
  File "/home/airflow/.local/lib/python3.13/site-packages/airflow/providers/standard/operators/python.py", line 254, in execute_callable
  File "/home/airflow/.local/lib/python3.13/site-packages/airflow/sdk/execution_time/callback_runner.py", line 97, in run
  File "/opt/airflow/repo/src/etl/customers.py", line 36, in transform_customers
  File "/home/airflow/.local/lib/python3.13/site-packages/pandas/core/series.py", line 5084, in apply
  File "/home/airflow/.local/lib/python3.13/site-packages/pandas/core/apply.py", line 1520, in apply
  File "/home/airflow/.local/lib/python3.13/site-packages/pandas/core/apply.py", line 1578, in apply_standard
  File "/home/airflow/.local/lib/python3.13/site-packages/pandas/core/base.py", line 1020, in _map_values
  File "/home/airflow/.local/lib/python3.13/site-packages/pandas/core/arrays/arrow/array.py", line 1770, in map
  File "/home/airflow/.local/lib/python3.13/site-packages/pandas/core/arrays/base.py", line 2745, in map
... [error truncated: 478 more characters]
```

## Root cause

Task customers_sync.transform failed with ValueError: unconverted data remains: T08:30:00Z raised in src/etl/customers.py::parse_signup_date (line 19). The input data satisfies the approved contract, so this is a defect in the code: it does not handle input that the contract allows. A minimal fix plus a regression test should resolve it.

## The failing test

`tests/test_customers_signup_date.py` reproduces the failure: it fails on the base branch and passes with the fix.

> **Note:** the tests/ folder was created because the repository had no test suite. Consider running pytest in CI so this test keeps guarding the fix.

```python
"""Regression test for customers_sync: the CRM export may send ISO-8601 signup timestamps."""

from datetime import date

from src.etl.customers import parse_signup_date


def test_parse_signup_date_accepts_iso_timestamp_with_z():
    assert parse_signup_date("2026-10-02T14:03:00Z") == date(2026, 10, 2)


def test_parse_signup_date_still_accepts_plain_dates():
    assert parse_signup_date("2025-01-14") == date(2025, 1, 14)
    assert parse_signup_date(" 2025-02-03 ") == date(2025, 2, 3)
```

## The fix

```diff
diff --git a/src/etl/customers.py b/src/etl/customers.py
index 2ef48ac..fb429d4 100644
--- a/src/etl/customers.py
+++ b/src/etl/customers.py
@@ -15,8 +15,11 @@ def extract_customers(**kwargs):
 
 
 def parse_signup_date(value):
-    """Parse the signup date sent by the CRM export (always YYYY-MM-DD)."""
-    return datetime.strptime(value.strip(), "%Y-%m-%d").date()
+    """Parse the signup date sent by the CRM export (YYYY-MM-DD or an ISO-8601 timestamp)."""
+    value = value.strip()
+    if value.endswith("Z"):  # fromisoformat() only accepts "Z" from Python 3.11
+        value = value[:-1] + "+00:00"
+    return datetime.fromisoformat(value).date()
 
 
 def normalize_phone(value):
diff --git a/tests/test_customers_signup_date.py b/tests/test_customers_signup_date.py
new file mode 100644
index 0000000..3cd3a10
--- /dev/null
+++ b/tests/test_customers_signup_date.py
@@ -0,0 +1,14 @@
+"""Regression test for customers_sync: the CRM export may send ISO-8601 signup timestamps."""
+
+from datetime import date
+
+from src.etl.customers import parse_signup_date
+
+
+def test_parse_signup_date_accepts_iso_timestamp_with_z():
+    assert parse_signup_date("2026-10-02T14:03:00Z") == date(2026, 10, 2)
+
+
+def test_parse_signup_date_still_accepts_plain_dates():
+    assert parse_signup_date("2025-01-14") == date(2025, 1, 14)
+    assert parse_signup_date(" 2025-02-03 ") == date(2025, 2, 3)
```

## Test results

**Before the fix (base branch + the new test, expected to fail):**

```text
$ pytest tests/test_customers_signup_date.py
exit code 1: 1 passed, 1 failed, 0 errors, 0 skipped, 2 collected in 0.0s
[pytest output reduced by the golden snapshot test to its E/FAILED/ERROR lines]
E           ValueError: unconverted data remains: T14:03:00Z
FAILED tests/test_customers_signup_date.py::test_parse_signup_date_accepts_iso_timestamp_with_z
1 failed, 1 passed
```

**After the fix (the new test and the existing suite):**

```text
$ pytest tests/test_customers_signup_date.py
exit code 0: 2 passed, 0 failed, 0 errors, 0 skipped, 2 collected in 0.0s
[pytest output reduced by the golden snapshot test to its E/FAILED/ERROR lines]
2 passed
```

## CI

CI has not run for this change. Its commit carries \[skip ci\], so GitHub starts no workflow triggered by `push` or `pull_request` for it, and many other CI services skip it too: the model-written test and fix do not run in CI, with the repository's secrets, before a person has reviewed them.

This branch is only in the local repository so far. Push it from there first (`git push --no-verify origin healer/customers_sync-transform-00000000`; the `push` and `pull_request` workflows do not run for that push either), then open the pull request with this description. The push and the pull request do start the workflows that the marker does not stop (`create` and `pull_request_target`): run `dag-healer doctor` with a GitHub token first, which reports those that can run this branch's code with the repository's secrets.

After your review, start CI with an empty commit on the branch, from a clone of the repository (in a POSIX shell such as bash):

```sh
git fetch origin healer/customers_sync-transform-00000000
commit=$(git commit-tree -p FETCH_HEAD -m "Run CI after review" "FETCH_HEAD^{tree}")
git push --no-verify origin "$commit":refs/heads/healer/customers_sync-transform-00000000
```

The commit is made without checking the branch out, so none of your git hooks runs with this branch's code in your working tree: `git commit-tree` runs no hook, and `--no-verify` skips your pre-push hook. A base branch that requires the CI checks blocks the merge until they pass. When you merge, keep the skip marker out of the message of the commit that reaches the base branch (a squash merge copies the commit messages by default), or CI does not run there either.

## Notes

- Branch `healer/customers_sync-transform-[number]` was pushed to the local repository.
- Repair agent summary: parse_signup_date hardcoded the "%Y-%m-%d" format, so the ISO-8601 timestamps that the CRM export now sends (for example 2026-10-02T14:03:00Z) raised ValueError. It now parses the value with datetime.fromisoformat (mapping a trailing "Z" to "+00:00") and still returns a date; plain dates keep working. The regression test and the whole suite pass.

## Review checklist

- [ ] The failing test reproduces the reported failure and would catch a regression.
- [ ] The fix addresses the root cause and is limited to what the test needs.
- [ ] No unrelated files, secrets or data files are changed.
- [ ] The full test suite passes in CI.
- [ ] After merging, retry the failed task (Healer tab, "Retry task") to confirm the pipeline recovers. Retrying also runs the tasks after it that failed or were blocked by it.

---

Generated by dag-healer <version> for incident `00000000000000000000000000000002`.
