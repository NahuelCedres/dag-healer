# Phase 1 of 2: write ONE failing test that reproduces the incident

{{a_product}} {{step}} of this repository failed and dag-healer classified the failure as a CODE
bug: the input data satisfies the approved data contract, so the code has to change. Before
anybody touches the code, you write a pytest test that reproduces the failure. A second phase
(with a new prompt) will fix the code; you must NOT fix anything now.

## What to do

1. Read the evidence at the end of this message, then the code it points to with `read_file`,
   `search` and `list_files`. Identify the smallest unit (function or method) that misbehaves.
2. Create exactly ONE new test file, `{{test_file}}`, with `write_file`. It contains one focused
   test (at most two or three closely related cases) that:
   - calls the misbehaving code directly, with hard-coded inputs copied from the evidence
     (exception message, traceback, masked sample rows). Prefer the exact value that broke
     the {{step}}, for example the string quoted in the error message;
   - asserts the CORRECT behaviour, i.e. what the code must return once it is fixed, so the
     test fails now and passes after a correct fix. Do not assert that the error is raised and
     do not use `pytest.raises` for the observed error;
   - is deterministic, fast and self-contained: no network, no database, no {{product}}, no
     current date or time, no reading of files under `data/` or `fixtures/`, no environment
     variables; only the standard library, pytest and packages the code under test already
     imports;
   - follows the repository's test style: {{test_style}};
   - imports the code under test like this: {{import_hint}};
   - has a descriptive name (`test_<behaviour>`) and a one-line docstring that names the
     incident's symptom.
3. Run it with `run_tests(["{{test_file}}"])` and check that it FAILS for the right reason: the
   same exception or the same wrong value the evidence shows. An `ImportError`, a
   `SyntaxError`, a `NameError`, a missing fixture or a typo in the test is NOT the right
   reason; neither is a test that passes. Correct the test (with `write_file` or
   `replace_in_file`) and run it again until it fails for the right reason.
4. Stop as soon as the test fails for the right reason, and finish with a short plain-text
   summary: the test name, what it asserts and the failure you observed.

{{created_note}}

## Rules (the tools enforce them and return an error message when you break one)

- In this phase you may create or change files ONLY under `{{tests_dir}}/`. Application code,
  {{pipelines}}, configuration, data and fixtures are read-only: the fix happens in the next phase.
- Paths are relative to the repository root. Absolute paths and `..` are rejected.
- Never written: `healer.yaml`, `healer/**`, `.github/**`, `.git/**`, lockfiles and dependency
  manifests (`requirements*.txt`, `pyproject.toml`, `setup.py`, `setup.cfg`, ...), `.env*`,
  `data/**`, `fixtures/**` and data files (`.csv`, `.parquet`, `.jsonl`, ...).
- Never read: `.git/**`, `.env*`, `healer.yaml`, `data/**`, `fixtures/**` and data files. Use the
  masked sample in the evidence instead.
- Masked values such as `h:1a2b3c4d5e`, `***`, `[email]`, `[phone]` or `[number]` are
  placeholders. Do not build assertions on them unless the code under test does not care about
  the exact value.
- Never weaken the reproduction: no `skip`, `xfail`, `try`/`except` around the failing call, no
  mocking of the code under test, no assertions that merely restate the current (buggy) result.

## Tools

You have these tools: {{tool_names}}.

- `list_files(path, pattern)`: list repository files (recursively) under a folder.
- `read_file(path, start_line, end_line)`: read a text file; lines come prefixed with their
  number and a `|`, which is not part of the file.
- `search(pattern, path)`: regular-expression search over the repository's text files.
- `write_file(path, content)`: create or overwrite a file (only under `{{tests_dir}}/` now).
- `replace_in_file(path, old, new)`: replace exactly one occurrence of `old` in a file.
- `run_tests(paths)`: run pytest on the given files (default: the tests folder) and get a
  summary plus the output tail.

## Evidence (masked)

{{evidence}}

{{previous_attempts}}
