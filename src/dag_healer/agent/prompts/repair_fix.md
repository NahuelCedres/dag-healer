# Phase 2 of 2: make the failing test pass with a minimal fix

{{a_product}} {{step}} of this repository failed because of a CODE bug. In phase 1 a pytest test was
written that reproduces the failure: `{{test_file}}`. It currently fails like this:

{{pytest_before}}

Your job now is the smallest correct fix of the application code.

## What to do

1. Read the failing test (below) and the code under test with `read_file`, `search` and
   `list_files`. Find the root cause; do not just silence the symptom.
2. Apply a minimal fix, preferably with `replace_in_file` (one focused edit per call):
   - the smallest diff that fixes the root cause for every input the test and the evidence
     describe, not only the exact test value; do not special-case the test's inputs and do not
     detect that a test is running;
   - keep the function names, signatures, return types and the module's public API;
   - keep the surrounding style (naming, quotes, indentation, comments, line endings); no
     reformatting, no reordering of imports, no renames, no unrelated clean-ups;
   - no new dependencies: only the standard library and the packages the module already
     imports; never edit requirements, lockfiles or packaging files;
   - do not swallow exceptions or return placeholder values just to make the test pass; invalid
     input that the old code rejected should still be rejected.
3. Run the new test with `run_tests(["{{test_file}}"])`, then the whole suite with
   `run_tests()`. Both must pass. If something fails, read the output, adjust the fix and run
   the tests again.
4. Finish with a short plain-text summary: the root cause, the change you made and the final
   test results.

## Rules (the tools enforce them and return an error message when you break one)

- You may change files ONLY under: {{allowed_paths}}.
- The new test and everything under `{{tests_dir}}/` are read-only in this phase, as is any other
  test file or conftest.py: never change a test to make it pass.
- Paths are relative to the repository root. Absolute paths and `..` are rejected.
- Never written: `healer.yaml`, `healer/**`, `.github/**`, `.git/**`, lockfiles and dependency
  manifests (`requirements*.txt`, `pyproject.toml`, `setup.py`, `setup.cfg`, ...), `.env*`,
  `data/**`, `fixtures/**` and data files (`.csv`, `.parquet`, `.jsonl`, ...).
- Never read: `.git/**`, `.env*`, `healer.yaml`, `data/**`, `fixtures/**` and data files.
- Size limit of the change: {{diff_limits}}. A larger change is rejected.
- Masked values such as `h:1a2b3c4d5e`, `***`, `[email]`, `[phone]` or `[number]` are
  placeholders for personal data; never hard-code them in the fix.

## Tools

You have these tools: {{tool_names}}.

- `list_files(path, pattern)`: list repository files (recursively) under a folder.
- `read_file(path, start_line, end_line)`: read a text file; lines come prefixed with their
  number and a `|`, which is not part of the file.
- `search(pattern, path)`: regular-expression search over the repository's text files.
- `write_file(path, content)`: create or overwrite a whole file (prefer `replace_in_file`).
- `replace_in_file(path, old, new)`: replace exactly one occurrence of `old` (copied verbatim
  from the file, without the line-number prefix) with `new`.
- `run_tests(paths)`: run pytest on the given files (default: the tests folder) and get a
  summary plus the output tail.

## The failing test: `{{test_file}}`

{{test_code}}

## Evidence (masked)

{{evidence}}

{{previous_attempts}}
