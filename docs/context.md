# The Organization Context and the ETL Information

A data contract says what the input of a pipeline must look like. It cannot say what the data
means, who produces it, or what a fix must never do. The **Organization Context** and the **ETL
Information** carry that knowledge: Markdown files in the DAG repository, written and reviewed by
the team like code. dag-healer reads them before every classification and every fix, the model
cites the rules it applies, and the code checks every citation.

The examples on this page come from Acme Health, a fictional health-tech company that runs the
data pipelines of its client, Acme General Hospital.

## The two files

| | Organization Context | ETL Information |
|---|---|---|
| File | `healer/organization.md` | `healer/etl/<pipeline id>.md`, one per pipeline |
| Holds | What the business does, its systems and their owners, a glossary, regulations, the rules for every pipeline | What one pipeline needs: its sources, its rules, known changes, how to recover |
| Rule IDs | `ORG-1`, `ORG-2`, ... | The file's own prefix: `LAB-1`, `LAB-2`, ... |

The pipeline id is the one the incidents use: the `dag_id` for Airflow, and for AWS Step Functions
the pipeline id of the adapter (the key under `orchestrator.options.pipelines`, or the state
machine's name with `discover: true`). The paths are settings of `healer.yaml` (see
[Configuration](#configuration)).

The ETL Information builds on the Organization Context: dag-healer always reads both, and a rule
of the ETL Information can replace a rule of the Organization Context for its pipeline
([Overriding a rule](#overriding-a-rule-of-the-organization-context)).

A minimal Organization Context:

```markdown
---
owner: "@acme/data-eng"
reviewed: 2026-10-20
review_every: 90d
---

# Acme Health: organization context

## What we do

Acme Health runs the data pipelines of its client, Acme General Hospital: lab results, patients,
pharmacy stock, encounters and claims, and the clinical dashboards built on them.

## Systems and owners

- Acme General Hospital, a client, is an external producer: the lab, registration and pharmacy
  exports. Contact it only through its account manager at Acme Health, Slack #client-acme-general.
- Data platform and dashboards: data engineering, Slack #data-eng. On-call: #data-oncall.

## Glossary

- **MRN**: the hospital's medical record number of a patient. **Finalized result**: a lab result
  with status F; P means preliminary, C corrected.

## Rules for every pipeline

- ORG-1: Patient identifiers (name, MRN, date of birth) are personal health data. They are masked
  everywhere: samples, alerts, logs, tests and pull requests. Tests use made-up values.
- ORG-2: Acme General Hospital is a client and is never contacted directly. dag-healer alerts
  on-call, and the account manager decides what to tell the hospital.
- ORG-3: Clinical values (results, vitals, doses) are never imputed or defaulted: no `fillna(0)`
  on clinical columns.
- ORG-4: A wrong number on a clinical dashboard is worse than a late one. Invalid records fail the
  task with a clear message; they are never dropped or fixed silently.
- ORG-5: Make the smallest change that makes the failing test pass. Files from before a change
  must still load, because old dates are run again.
```

And the ETL Information of one pipeline, `healer/etl/lab_results_daily.md`:

```markdown
---
pipeline: lab_results_daily
owner: "@acme/clinical-data"
reviewed: 2026-10-20
review_every: 90d
rule_prefix: LAB
---

# lab_results_daily

Builds on the Organization Context (healer/organization.md).

Loads the hospital's lab results into the results mart every morning. Clinicians read the lab
dashboard from 07:00.

## Sources

- `data/incoming/lab_results.csv`: the export of the hospital's lab system (ORG-2).

## Rules

- LAB-1: A finalized result always has a value; a preliminary one may be empty.
- LAB-2: A value only means something with its unit: a fix never converts values or compares them
  across units.

## Recovery

- LAB-3: After the hospital sends the export again, run extract_lab_results and everything
  downstream again: transform_lab_results alone reads the old staging copy.
```

LAB-1 is the kind of rule a contract cannot express: a column that may be empty only when another
column says so. Rules that a contract can check (a column that is never empty, a range) belong in
the contract too, where dag-healer enforces them.

## The front matter

Each file opens with a YAML front matter between two `---` lines:

| Key | Files | Value |
|---|---|---|
| `owner` | both | Who keeps the file up to date, such as `"@acme/data-eng"` |
| `reviewed` | both | The date of the last review, `YYYY-MM-DD` |
| `review_every` | both | How often to review it: a number and `d`, `w`, `m` or `y`, such as `90d` or `6m` (months and years are calendar ones) |
| `rule_prefix` | ETL Information | The prefix of the file's rule IDs: 2 to 10 capital letters or digits, a letter first, not `ORG` |
| `pipeline` | ETL Information, optional | The pipeline id; when given, it must match the file's name |

A file is **overdue for a review** once `reviewed` plus `review_every` is in the past. dag-healer
still reads it, notes it in the incident, marks its prompt block `review="overdue since ..."`, and
`dag-healer doctor` warns about it. A missing or invalid key is a problem of the file (see
below); without a valid `rule_prefix`, no rule of an ETL Information can be cited.

## Rules and their IDs

A rule is a list item whose text starts with its ID and a colon:

```markdown
- LAB-1: A finalized result always has a value; a preliminary one may be empty.
```

- The item starts with `-`, `*` or `+`, indented 3 spaces at most. The lines indented under it,
  and the lines that follow it without a blank line, belong to the rule.
- An ID is the prefix, `-` and a number from 1 to 9999 without leading zeros: `ORG-3`, `LAB-12`.
  The Organization Context uses `ORG`; an ETL Information uses its `rule_prefix`.
- A rule can stand in any section: `## Rules`, `## Recovery`, `## Calendar`.
- IDs are stable: the model cites them, PR.md lists them and the decision log keeps them. Give a
  new rule the next free number, and never reuse the number of a rule you deleted.
- HTML comments (`<!-- ... -->`) and fenced code blocks never hold rules. Comments are also left
  out of what the model reads (each one counts as a space, so it never joins the words around
  it), so they are the place for notes to the team.

dag-healer parses the files strictly and reports, with the line, everything that looks like a rule
but is not one: `ORG1`, `org-1`, `ORG-01`, `**ORG-1**`, an ID without its colon, an ID in a heading
(`## ORG-3: ...`), an indented item, an ID of another prefix, an `ORG` ID inside an ETL
Information, and an ID defined twice (the first definition counts). The file is still used; only
its well-formed rules can be cited. To write a code that is not a rule at the start of an item, put
it in backticks: `` - `PHA-17`: the code of a discontinued pharmacy item ``.

## Overriding a rule of the Organization Context

A rule of the ETL Information replaces a rule of the Organization Context for its pipeline when it
names it in parentheses after its ID:

```markdown
- PHA-2 (overrides ORG-3): An empty `quantity` means the item is out of stock: the export leaves it
  empty instead of writing 0. Stock is not a clinical value, so filling it with 0 is right.
```

- Only an ETL Information overrides, and only `ORG` rules that the Organization Context defines:
  anything else is a problem of the file. One rule may override several: `(overrides ORG-3, ORG-4)`.
- The model reads both rules and the override. PR.md shows the overridden rule with "replaced here
  by PHA-2".

## What dag-healer does with them

1. **Reading.** Every triage of an incident reads the Organization Context and the pipeline's ETL
   Information from the worker's checkout of the repository, from the commit of
   `repo.base_branch`: the newer of the local branch and `origin/<base_branch>` (a checkout that
   fetches without updating its local branch reads the fetched commit; in a shallow clone, such as
   git-sync's, the one with the newer commit date; when the two have diverged, the local branch is
   read and the incident says so), else `HEAD`, which a git-sync checkout uses. It never reads the
   working tree: commit a change to the base branch before dag-healer uses it. A file that is in
   the working tree but not in that commit gets a note in the incident, and so does a checkout
   whose `HEAD` holds other context files than that commit (another branch, or a local base branch
   that nothing updates). Each file read is recorded with its path and the commit (`path@sha`).
2. **Refusing.** First, the characters that show nothing (zero-width spaces and joiners, soft
   hyphens, bidi controls, tag characters) are taken out of the file, so they cannot hide a secret,
   an e-mail address or a prompt tag; a joiner that an emoji or a script such as Persian needs
   stays. Then a file that holds what looks like a secret is refused: nothing of it reaches the
   model or the store. dag-healer looks for private keys, AWS access keys, GitHub, GitLab,
   Anthropic, Slack, Google, Stripe, Azure storage, Databricks, Vault, npm and SendGrid tokens,
   `Authorization` headers with a token, passwords in URLs, credentials written with their value
   (`password: ...`, `PGPASSWORD=...`, or the value alone on the next line) and the values of the
   worker's credentials. Code that fetches a credential (`Variable.get(...)`,
   `settings.CRM_SECRET`), a path and words such as `stored-in-vault` are not values, so a rule
   that says where a credential lives is read. It looks at the file as it is, without its HTML
   comments (a comment can split a value), in fullwidth letters' ASCII form, and as the model
   would read it: a rule whose lines are joined, the owner, the text. A file larger than
   `context.max_file_bytes`, not UTF-8 text or a symbolic link is refused too. The incident and
   `dag-healer doctor` say why, without the value.
3. **Masking.** The text and the rules of the files that are read are masked like every other
   prompt input: e-mail addresses, phone numbers, IP addresses, IBANs and long numbers become
   placeholders before anything else sees them. Dates are not masked, birth dates included: keep
   them out of the files.
4. **Classifying.** The classification prompt holds an `<organization_context>` and an
   `<etl_information>` block, before the incident. The model uses them to understand the business
   and cites, in `rules_cited`, the rules that apply. dag-healer checks every cited ID against the
   files it read: an unknown ID is never counted as applied, and it is flagged.
5. **Never above the fixed rules.** The rules never override dag-healer's fixed rules: a DATA
   verdict still needs a violated, approved contract, a CODE verdict is still refused while an
   approved contract is violated, and infrastructure failures still go to a person. When the model
   says that the automatic follow-up of its verdict would go against a rule that dag-healer read
   (the alert of a DATA verdict, the automated fix of a CODE verdict), the incident waits for a
   person instead: "Needs a decision", with the rule and the reason. A conflict with a rule that
   was not read (an ID no file defines, or no file read at all), or with an ORG rule that the
   pipeline's ETL Information replaces, is flagged and changes nothing.
   Where the model decides on its own (a CODE verdict it confirms, the failures no fixed rule
   decides), what a rule says still weighs on its verdict, towards CODE too: a rule that calls a
   case a bug in the pipeline's code makes CODE more likely. Write rules about what a fix may do,
   not about how to classify.
6. **Repairing.** Both repair phases (the failing test, then the fix) get the same blocks, and the
   agent two more tools: `cite_rules`, for the rules that shaped its work, and `stop_for_rule`,
   when the only fix it sees would go against a rule. A stop ends the repair before anything is
   committed or pushed, even when the session fails after it, and the incident waits for a person
   with the rule and the reason. A stop for an ORG rule that the ETL Information replaces is
   refused, and the agent is pointed to the rule that replaces it. The agent's file tools never read the context files (it has them
   in the prompt, masked), not even through a link, and never change them: they are protected
   paths, like the contracts.
7. **Reporting.** The pull request description (and PR.md) has a "Rules applied" section: each rule
   that the classification, the accepted test and fix, or a stop cited (an attempt that was
   rejected and rolled back does not count), with a short text and `path@sha`, then the cited IDs
   that do not exist, flagged.
8. **Remembering.** The store keeps what each triage read (the context snapshot: paths, commits,
   front matter, parsed rules, problems, the masked text, and the refused files with their
   reasons; a triage that reads the same files again reuses it) and the **decision log**: every
   classification (by a fixed rule such as `a`, `b`, `c`, `d` or `infra`, or by the model that
   answered, a fallback model included, with its confidence), every human action (by the person
   who took it, with their note), every contract approval (from the console or with
   `dag-healer contracts approve`) and every repair outcome (fix proposed, fix failed, stopped by a
   rule conflict), each with the rules it cited. The decision log stays when an incident is
   deleted. The console does not show them yet: they are in the store, so a view needs no access
   to the worker's checkout.

### After a stop for a rule

The incident waits for a person, with the rule and the reason. The way forward:

- **The rule is wrong or too broad:** change it in a pull request to the base branch. Once it is
  merged, "Resolve as CODE" runs the repair again, and the repair reads the new rule.
- **The fix is right but needs a person:** fix it by hand in a pull request of your own. Once it
  is deployed, retry the task, or ignore the incident.
- **Nothing should change:** ignore the incident.

"Resolve as CODE" with the rule unchanged runs the same repair, and the agent may stop again for
the same rule: the note you write with it does not reach the agent yet.

docs/security.md (section 4.7) describes the context as prompt input: what an author of the files
can and cannot make dag-healer do.

## Configuration

```yaml
context:
  organization: healer/organization.md   # the Organization Context
  etl_dir: healer/etl                    # <etl_dir>/<pipeline id>.md
  max_file_bytes: 16384                  # a larger file is refused (1024 to 262144)
```

These are the defaults, so the section is optional. Both paths are relative to `repo.path` and
stay inside the repository, out of `.git`. The Organization Context cannot be the ETL Information
folder or a file in it (compared without case, as macOS and Windows compare names).
`repo.allowed_paths` cannot name them: a fix never changes the context files.

## Starter files

`dag-healer init --context` writes a starter Organization Context and one starter ETL Information
for every pipeline it finds, with the sections above and guiding questions as comments. It never
overwrites a file that exists, and it gives every ETL Information a rule prefix of its own:

```console
$ dag-healer init --context
...
Context:       healer/organization.md: written
               healer/etl/lab_results_daily.md: written (rules LAB-1, LAB-2, ...)
               healer/etl/patients_sync.md: written (rules PAT-1, PAT-2, ...)
               Answer the questions in the starter files, then commit them to main: dag-healer reads the committed files only.
```

A starter file has no rule until you write one: its examples are inside comments.

## Checking the files

`dag-healer doctor` reads the files as the worker does and reports them on its `context` line:

```text
[ OK ] context  main@9e41c07: healer/organization.md (5 rules); 2 of 2 ETL Information file(s) in healer/etl/ (5 rules)
[WARN] context  main@9e41c07: healer/organization.md (5 rules); 2 of 2 ETL Information file(s) in healer/etl/ (4 rules); invalid: healer/etl/patients_sync.md (1: line 14: PAT1 is not a rule ID; write it as PAT-1); not committed, so not read: healer/etl/pharmacy_stock_report.md
[FAIL] context  main@9e41c07: ...; refused, they hold what looks like a secret (remove it from the history and rotate it): healer/etl/claims_settlement.md (it holds what looks like a password in a URL on line 9; ...)
```

It warns about invalid files, files overdue for a review, files refused for their size or
encoding, edits and files that are not committed, a base branch it could not find, a base branch
that has diverged from its `origin` copy, and a checkout whose `HEAD` holds other context files. A secret is an error: remove it from the file and
from the repository's history, and rotate it. Without any context file the line is skipped.

## Writing good rules

- One rule per item, in plain sentences: what must hold, and what a fix may or may not do.
- Say who owns what and who decides: the model's verdicts and stops cite those rules.
- Keep each file short (16 KB by default) and specific. The Organization Context is read for every
  pipeline, so pipeline details belong in the ETL Information.
- Never write secrets: the file is refused. Avoid personal data: it is masked, but the file is
  better without it.
- Review the files like code, with a code-owner review: whoever can change them can steer the
  agent, within the fixed rules.
