# Security policy

dag-healer reads task logs and data from your pipelines, sends a masked version of them to a
language model and runs the tests that the model writes. Its security model and the known gaps
of the current version are described in [docs/security.md](docs/security.md). Read it before you
deploy dag-healer.

## Supported versions

dag-healer is in early development (alpha).

| Version | Supported |
|---|---|
| 0.1.x (alpha) | Yes |

Fixes land on `main` and ship in the next 0.1.x release. Earlier releases get no backports.

## Reporting a vulnerability

Do not report a vulnerability in a public issue, discussion or pull request. Use GitHub's
private vulnerability reporting instead:

1. Open the **Security** tab of this repository.
2. Click **Report a vulnerability**.
3. Fill in the form and submit it.

Only you and the repository's maintainers can see the report. Include what you can of:

- the affected version or commit;
- the relevant configuration: `healer.yaml` with every secret removed, and how the worker is
  deployed;
- the steps to reproduce the problem, or a proof of concept;
- what an attacker gains, and what they need to start with (for example: control over what a
  task writes to its log, an Airflow user with read access, or the ingest token).

Use synthetic data only. Never put real credentials, unmasked logs or personal data in a report.

## What to expect

dag-healer is an early-stage project, maintained on a best-effort basis. There is no
service-level agreement for responses or fixes.

- Each report is read, then confirmed or declined in its advisory thread.
- A confirmed vulnerability is fixed on `main` and released in a new version, together with a
  GitHub security advisory.
- Reporters are credited in the advisory unless they prefer not to be.
- Please keep the details private until the advisory is published.

## Scope

In scope: the `dag-healer` package (the Airflow plugin, the HTTP API, the UI, the worker and the
repair agent) and its documented defaults.

[docs/security.md](docs/security.md) lists the known gaps of 0.1.x. One example: the pytest
sandbox does not isolate hostile code. Reports that show a new way to exploit a known gap, or a
way around a documented mitigation, are welcome.

Out of scope:

- The development stack under `docker/`. It uses fixed, publicly known secrets by design and
  must never be exposed.
- Vulnerabilities in Apache Airflow, the Anthropic API or other dependencies. Report those to
  their projects, but do report here if the way dag-healer uses a dependency makes a
  vulnerability exploitable.
- Attacks that require control of `healer.yaml`, the worker's environment or the masking hook.
  Administrators control these inputs, and dag-healer trusts them.
