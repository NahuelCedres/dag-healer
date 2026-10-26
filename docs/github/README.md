# Setting up a GitHub repository for dag-healer

dag-healer proposes each CODE fix as a pull request on your repository. This page sets the
repository up for it: the token, the rules of the base branch, how the repository's CI runs on
dag-healer's pull requests, and what `dag-healer doctor` reports. The examples use the
repository `acme-health/etl` and the base branch `main`. [docs/security.md](../security.md)
explains the risks behind this advice (section 4.5, gaps G2 and G3).

## What dag-healer does on GitHub

With a token (`github.token`, usually `${GITHUB_TOKEN}`) and a repository (`github.repo`, or a
GitHub `origin` remote), a CODE repair ends like this:

1. dag-healer commits the new test and the fix on a new branch,
   `healer/<dag_id>-<task_id>-<first 8 characters of the incident id>`, in a clone of its own.
   The commit message carries `[skip ci]` (see "How CI runs after review"). The commit itself is
   then checked: it must touch none of the protected paths (see "Protected paths") and its stored
   message must still hold the marker, or nothing is pushed.
2. It checks, read-only, how the token, the repository and its workflows are set up (see "What
   doctor reports").
3. It pushes that commit as the branch to the repository itself, not to a fork, and opens a pull
   request into `github.pr_base` (default `main`). When step 2 found problems, the description
   lists them first, under "Before you merge". They never stop the pull request.

The workflows that `[skip ci]` does not stop (see below) start with step 3, before anyone reads
the pull request and its "Before you merge" section. Run `dag-healer doctor` with the token,
and make its `github ...` lines clean, before you give the token to the worker.

dag-healer never merges, never force-pushes and never pushes to the branch it cloned: a person
merges the pull request.

Without a token or a repository, dag-healer sends nothing to GitHub. It pushes the branch into
the local repository and writes `PR.md` and `fix.patch` under `<healer_home>/prs/<incident_id>/`.
The commit carries `[skip ci]` there too, so CI still waits for a review when someone pushes the
branch later.

## The token

Create a fine-grained personal access token (Settings > Developer settings > Personal access
tokens > Fine-grained tokens > Generate new token):

- **Resource owner:** the account or organization that owns the repository (`acme-health`). An
  organization may have to approve the token.
- **Repository access:** Only select repositories, with this repository only
  (`acme-health/etl`).
- **Repository permissions:**
  - Contents: Read and write, to push the `healer/...` branch;
  - Pull requests: Read and write, to open the pull request (or find the one already open);
  - Metadata: Read-only, which GitHub adds by itself;
  - nothing else: in particular no Workflows (without it, GitHub refuses any push that changes
    `.github/workflows/`) and no Administration, which would not even let dag-healer read the
    classic branch protection of the base branch: GitHub shows that only to an admin of the
    repository, which the token's account should not be (a ruleset needs no extra permission).

Create the token under an account of its own, such as a machine account with the Write role on
the repository (not Admin). dag-healer's pull requests are opened by that account, and GitHub
does not let the author of a pull request approve it, so the reviewers need accounts of their
own.
Give the token to the worker in the variable that `github.token` names (`GITHUB_TOKEN` in the
`healer.yaml` that `dag-healer init` writes), never as a literal value in `healer.yaml`.

A GitHub App works too: install it on this repository only (Only select repositories), with the
same repository permissions. dag-healer does not create installation tokens: it uses the token
as it is given, and an installation token expires after one hour. An App therefore suits a
worker that gets a fresh token every time it starts, such as `dag-healer worker --once` run by a
scheduler.

Avoid classic personal access tokens (`ghp_...`) and OAuth tokens (`gho_...`): with the `repo`
scope, they reach every repository their account can write to. Avoid GitHub App user tokens
(`ghu_...`) too: they act for their user on every repository that the App is installed on and
the user can access. doctor and every pull request warn about all three.

## The rules of the base branch

[`base-branch-ruleset.json`](base-branch-ruleset.json) is a repository ruleset for the default
branch. It requires a pull request with one approving review before a merge, dismisses stale
approvals when new commits are pushed, requires the most recent push to be approved by someone
other than the person who pushed it, blocks force pushes and deletion, and has an empty bypass
list: nobody, repository admins included, can skip its rules (admins can still change the
ruleset itself).

Create it as an administrator of the repository (dag-healer's token cannot, and should not be
able to):

- in the browser: Settings > Rules > Rulesets > New ruleset > Import a ruleset, choose the file,
  review the rules and click Create;
- or with the GitHub CLI, from the folder that holds the file:

  ```sh
  gh api repos/acme-health/etl/rulesets --method POST --input base-branch-ruleset.json
  ```

The ruleset targets the default branch (`~DEFAULT_BRANCH`). When `github.pr_base` is another
branch, replace `~DEFAULT_BRANCH` with `refs/heads/<that branch>` in the file before you create
the ruleset. Keep it Active: GitHub does not apply a ruleset in Evaluate or Disabled mode, and
doctor does not count it.

### Required status checks

The file holds no status checks, because their names depend on your CI. Add them once CI has run
on a pull request: edit the ruleset, turn on "Require status checks to pass", and add each check
by the name that GitHub shows on the pull request (for example `unit-tests`). Or add this rule to
`rules` before you create the ruleset:

```json
{
  "type": "required_status_checks",
  "parameters": {
    "strict_required_status_checks_policy": false,
    "required_status_checks": [{"context": "unit-tests"}]
  }
}
```

With `github.skip_ci: true` (the default), CI does not run on a healer pull request until a
person starts it (see the next section). A required check keeps the pull request from being
merged until CI has run and passed. Without one nothing does, and doctor warns about it.

Classic branch protection (Settings > Branches) works too: require a pull request with one
approval, require the CI's status checks, leave force pushes and deletions off, and turn on "Do
not allow bypassing the above settings". GitHub shows classic protection only to a token whose
account is an admin of the repository (which this page advises against), or to a GitHub App
with Administration: Read-only. With any other token, doctor cannot tell whether the protection
meets these rules and asks you to check it by hand: protect the branch with the ruleset instead,
or check the protection yourself.

## How CI runs after review

By default (`github.skip_ci: true`), the message of dag-healer's commit opens its first
paragraph after the title with this line:

```text
[skip ci] CI runs after a person has reviewed this change (see the pull request).
```

GitHub then starts no workflow triggered by `push` or `pull_request` for that push, or for the
pull request while that commit is its head, and many other CI services skip the commit too. The
marker sits at the start of the message body so that services that read only the beginning of a
message find it as well. The pull request's "CI" section says that CI has not run and how to
start it.

After your review, start CI with an empty commit on the branch, from a clone of the repository
(in a POSIX shell such as bash; on Windows, Git Bash):

```sh
git fetch origin healer/customers_sync-transform-0123abcd
commit=$(git commit-tree -p FETCH_HEAD -m "Run CI after review" "FETCH_HEAD^{tree}")
git push --no-verify origin "$commit":refs/heads/healer/customers_sync-transform-0123abcd
```

`git commit-tree` makes a commit with the branch's files, unchanged, on top of the fetched
branch, without checking the branch out. The new commit carries no marker, so the `push` and
`pull_request` workflows run for it as usual. Your working tree never holds the branch's code,
so none of your git hooks runs with it: `git commit-tree` runs no hook, and `--no-verify`
skips your pre-push hook. (A checkout of the branch, with `git switch`, would run your
post-checkout hook in a working tree that holds the branch's code, and hook managers read
their configuration from that tree.)

- **PR.md mode.** The branch is only in the local repository. Push it from there first
  (`git push --no-verify origin <branch>`; the `push` and `pull_request` workflows do not run
  for that push either, but the ones of the next section do), open the pull request with the
  text of `PR.md`, then start CI as above. Run `dag-healer doctor` with a token first, to see
  what the push and the pull request would start (without a token, dag-healer checked nothing
  on GitHub). `PR.md` says so too. When the branch could not be pushed into the local repository
  either, only `fix.patch` holds the change: apply it on a new branch of that name, made from the
  base branch, and commit it with `git commit --no-verify` and a message whose first paragraph
  after the title opens with the marker, as `PR.md` then says, before you push it.
- **Approvals.** Push the empty commit before you approve: the ruleset may dismiss an approval
  given before a new push. With "Require approval of the most recent reviewable push", GitHub may
  also count your empty commit as that push and ask for the approval of someone else. A team with
  a single reviewer sets `require_last_push_approval` to `false` in the file before creating the
  ruleset.
- **Merging.** Keep the marker out of the message of the commit that reaches the base branch, or
  the base branch's CI skips that commit too. By default a squash merge copies the commit messages
  into its own: delete the marker line before you confirm the merge. A merge message built from
  the pull request description can also hold a marker that the model wrote into its diagnosis,
  notes, test or diff (dag-healer's own text never spells one): read the whole message.

With `github.skip_ci: false`, the commit carries no marker and the pull request has no CI
section: CI runs as soon as dag-healer pushes, before anyone has read the change. Use it only when
no workflow that runs for healer branches holds secrets or write permissions. doctor then warns
about each `push` or `pull_request` workflow that runs for healer branches and uses them.

## Workflows that `[skip ci]` does not stop

The marker stops only workflows triggered by `push` and `pull_request`. Two kinds of workflow can
still run the healer branch's code with the repository's secrets, and doctor reports both:

- **`pull_request_target`** runs with the repository's secrets whatever the commit message says.
  It is safe only while it never checks out or runs the pull request's code. doctor reports each
  one that runs for pull requests into the base branch and refers to the pull request's head:
  `github.event.pull_request.head` (its `sha`, `ref`, `repo`, ...), `github.head_ref`,
  `GITHUB_HEAD_REF`, `github.event.pull_request.merge_commit_sha`, a `refs/pull/` ref,
  `pull/<number>/head` or `pull/<number>/merge` (`git fetch origin pull/...`), or
  `gh pr checkout`. It is an error, whatever the activity `types`. Most types can start without
  anyone choosing to: `review_requested` fires when GitHub requests the code owners' review as
  the pull request opens. `labeled`, `unlabeled` and `closed` are errors too: a bot can label the
  pull request before anyone has reviewed it, and closing it is how a reviewer rejects it, so the
  rejected code runs with the secrets.
- **`create`** runs when dag-healer pushes its new branch, with that branch's code checked out by
  default. doctor reports it as a warning, or as an error when it uses secrets or write
  permissions.

A workflow uses secrets or write permissions when one of its expressions (`${{ ... }}`, or a
whole `if:` condition) names a secret other than `secrets.GITHUB_TOKEN`, when one of its jobs
passes `secrets: inherit`, or when it sets `permissions: write-all` or a permission to `write`,
at the workflow or the job level: `id-token: write` gives it OIDC tokens for cloud
credentials, `contents: write` a `GITHUB_TOKEN` that can push. Expressions and permissions
outside the jobs count for every job. A job that calls a reusable workflow of the same folder
(`uses: ./.github/workflows/build.yml`) counts what that workflow and the ones it calls do. The
default permissions of `GITHUB_TOKEN`, set in the repository's settings, are not read.

To keep such a workflow and leave healer branches out, give each of its jobs one of these
conditions:

```yaml
jobs:
  test:
    # pull_request_target (and pull_request): not for healer pull requests
    if: ${{ !startsWith(github.head_ref, 'healer/') }}
```

```yaml
jobs:
  test:
    # create (and push): not for healer branches
    if: ${{ !startsWith(github.ref, 'refs/heads/healer/') }}
```

doctor recognises a condition as one of these exact forms, either as the whole condition or as
one of the conditions joined by `&&` at its top level (`a && !startsWith(...)`). The same form
negated again, compared (`== false`), joined by `||`, or with text around the `${{ ... }}` skips
nothing for doctor. On a job that calls a reusable workflow, the condition goes on the calling
job: conditions inside the called workflow are not read.

doctor does not report a `create` workflow whose jobs run only for tags, by the condition
`startsWith(github.ref, 'refs/tags/')`, `github.ref_type == 'tag'` or
`github.event.ref_type == 'tag'`, in the same exact forms.

`branches` and `branches-ignore` filter different branches depending on the event. Under `push`
they filter the pushed branch, so `branches-ignore: ['healer/**']` leaves healer branches out.
Under `pull_request` and `pull_request_target` they filter the base branch, the one that the pull
request targets, so `branches-ignore: ['healer/**']` there does not leave out a healer pull
request: use the job condition on `github.head_ref` instead.

doctor reads only the files in `.github/workflows/`: the reusable workflows of other
repositories and the actions that the workflows use are not read, and neither is code that the
workflows run (a script that fetches the pull request's head by itself is not recognised).
Workflows triggered by `workflow_run`, `pull_request_review`, `pull_request_review_comment`,
`issue_comment`, `workflow_dispatch`, `merge_group` or `schedule` ignore the marker too, and
doctor does not check them: make sure that none of them runs the pull request's code with
secrets before the review is done.

## What doctor reports

With a token and a repository, `dag-healer doctor` adds these lines after its `github` line:

```text
[ OK ] github              pull requests on acme-health/etl (token hidden)
[ OK ] github token        a fine-grained personal access token
[ OK ] github repository   the token's account is not an admin of acme-health/etl
[ OK ] github base branch  main requires a pull request with an approving review and blocks force pushes and deletion, and requires status checks
[ OK ] github workflows    4 workflow files checked
```

A check that finds problems prints one line per problem instead, with what to change. `[FAIL]`
lines are errors and make doctor exit 1; `[WARN]` lines are warnings. A check that cannot run
(GitHub cannot be reached, answers 403 or 404, answers too slowly, or gives an answer that
dag-healer does not understand or that is larger than 2 MB), or that fails unexpectedly,
prints one warning that says what was not checked, never an OK, and the other checks still run;
when the checks cannot start at all (a proxy setting that cannot be used, for example), one
`github setup` warning takes their place. The same holds for the parts of a check: scopes that
GitHub's answer does not list, rulesets whose answer was lost, could not be read or was an error
other than a refusal (a server error, a rate limit, whether GitHub answers 429 or 403, a
redirect), and the rulesets beyond the first 10 that hold a required rule, which are not read,
are warnings too. Only a ruleset that GitHub refuses to show (it answers 403 or 404, unless the
403 is the rate limit's, for an organization ruleset for example), or whose answer does not say
whether the token's account can bypass it, is left out without a warning.

An OK line says what was checked, no more. For a fine-grained token, the kind is all that the
token line checks: GitHub does not tell which repositories it reaches or which permissions it
holds, so check by hand that it is limited to the one repository, without Workflows or
Administration: Read and write.

Before each pull request, dag-healer runs the same checks and lists the problems at the top of
the description, under "Before you merge", errors first. They never stop the pull request: fix
them before you merge. Without a token or a repository (PR.md mode), nothing is checked and
there is no such section.

| Line | Level | What it found | What to do |
|---|---|---|---|
| `github token` | warning | a classic personal access token, an OAuth token, a GitHub App user token, or a token whose kind its prefix does not tell | a fine-grained token limited to the repository, or a GitHub App installed on it only ("The token") |
| `github token` | warning | the `workflow` scope, or admin or delete scopes | remove them, or use a fine-grained token |
| `github token` | warning | scopes that could not be read (no `X-OAuth-Scopes` header, or no answer), for a classic, OAuth or unknown token | check the scopes by hand |
| `github token` | warning | a GitHub App installation that can reach more than one repository | install the App on this repository only |
| `github repository` | warning | the token's account is an admin of the repository | an account with at most the Write role |
| `github base branch` | error | no pull request with an approving review required, force pushes allowed, or deletion allowed (one line each) | the ruleset ("The rules of the base branch") |
| `github base branch` | warning | no required status checks (with `github.skip_ci: true`) | add the CI's checks ("Required status checks") |
| `github base branch` | warning | the token's account can bypass a ruleset, or is exempt from it | remove the account, or its role, team or app, from the ruleset's bypass list |
| `github base branch` | warning | a ruleset whose answer was lost, could not be read or was an error other than a refusal (a 403 of the rate limit is none), or rulesets beyond the first 10, so their bypass lists were not checked | check the rulesets' bypass lists by hand |
| `github base branch` | warning | classic branch protection that the token cannot read | check it by hand, or protect the branch with a ruleset instead ("The rules of the base branch") |
| `github workflows` | error | a `pull_request_target` workflow that refers to the pull request's head, whatever its activity types | never check out or run the head there, or skip healer pull requests ("Workflows that `[skip ci]` does not stop") |
| `github workflows` | warning, error with secrets or write permissions | a `create` workflow | skip healer branches, or run it for tags only |
| `github workflows` | warning | with `github.skip_ci: false`, a `push` or `pull_request` workflow that runs for healer branches and uses secrets or write permissions | set `github.skip_ci: true`, or skip healer branches |
| `github workflows` | warning | workflow files that were not checked: a symbolic link, larger than 1 MB, beyond 2 MB of workflow files in all, not readable, not UTF-8, not valid YAML, not a workflow, too large or complex to analyse (more than about 100 KB, or less when aliases and merge keys repeat parts of it, or a file whose loading or analysis takes more than 2 seconds), or beyond the first 200 files | check them by hand |
| `github setup` | warning | the checks could not start or failed unexpectedly (the line names the exception's type only) | check the network settings and `github.repo`, then check the token, the base branch's rules and the workflows by hand |

The checks send only these read-only requests, to `api.github.com`, with the configured token.
They never follow a redirect, so a renamed repository is reported instead of followed, and read
each answer uncompressed, up to 2 MB, within twice the request timeout; all of them share six
times that timeout. The right column is the permission that a fine-grained token needs for each
one:

| Request | When | Permission |
|---|---|---|
| `GET /repos/acme-health/etl` | always | Metadata: Read-only |
| `GET /installation/repositories` | GitHub App tokens only | none beyond the installation |
| `GET /repos/acme-health/etl/rules/branches/main` | always | Metadata: Read-only |
| `GET /repos/acme-health/etl/branches/main` | when the rules leave a requirement unmet | Contents: Read-only |
| `GET /repos/acme-health/etl/branches/main/protection` | when that branch is protected | Administration: Read-only, and an account that is an admin of the repository (a GitHub App: Administration: Read-only); otherwise doctor warns that it cannot read the protection |
| `GET /repos/acme-health/etl/rulesets/<id>` | for the rulesets whose rules meet a requirement | Metadata: Read-only |

Opening the pull request takes `POST /repos/acme-health/etl/pulls`, and, when one is already
open, `GET /repos/acme-health/etl/pulls` (Pull requests: Read and write). doctor reads the
workflow files of `repo.path`; before a pull request, dag-healer reads those of its clone.

## Protected paths

A fix never changes CI or git configuration. The repair agent's write tools refuse these paths,
whatever the phase:

- dag-healer's own files and inputs: `healer.yaml` at any depth, the contracts folder, and the
  root folders `healer/`, `data/` and `fixtures/`;
- CI definitions, at the root of the repository: `.github/`, `.gitlab-ci.yml`,
  `.gitlab-ci.yaml`, `.gitlab/`, `.circleci/`, `Jenkinsfile`, `azure-pipelines.yml`,
  `azure-pipelines.yaml`, `.azure-pipelines/`, `.buildkite/`, `buildkite/`, `buildkite.yml`,
  `buildkite.yaml`, `buildkite.json`, `bitbucket-pipelines.yml`, `.travis.yml`, `.drone.yml`,
  `.drone.yaml`, `.drone.star`, `.drone.jsonnet`, `.woodpecker.yml`, `.woodpecker.yaml`,
  `.woodpecker/`, `buildspec.yml`, `buildspec.yaml`, `cloudbuild.yaml`, `cloudbuild.yml`,
  `cloudbuild.json`, `appveyor.yml`, `.appveyor.yml`, `.cirrus.yml`, `.cirrus.star`,
  `.semaphore/`, `.teamcity/`, `.tekton/`, `.build.yml`, `.builds/`, `.gitea/` and `.forgejo/`;
- git hook managers and the configuration their hooks run, at the root:
  `.pre-commit-config.yaml`, `.pre-commit-config.yml`, `.husky/`, `.huskyrc*`, `.githooks/`,
  `.git-hooks/`, every file or folder whose name starts with `lefthook*` or `.lefthook*`,
  `.config/lefthook*`, `.overcommit.yml`, `.simple-git-hooks*`, `simple-git-hooks*`,
  `.lintstagedrc*`, `lint-staged.config.*`, `.commitlintrc*` and `commitlint.config.*`;
- development environments that run commands when someone opens the repository, at the root:
  `.devcontainer/`, `.devcontainer.json`, `.vscode/`, `.gitpod.yml` and `.gitpod.Dockerfile`;
- git internals and metadata, at any depth: `.git/`, `.gitattributes`, `.gitmodules` and
  `.lfsconfig`;
- code owners: `CODEOWNERS`, `docs/CODEOWNERS` and `.github/CODEOWNERS`;
- `.env*` files, lockfiles, dependency and packaging manifests (`pyproject.toml`, `setup.py`,
  `setup.cfg`, `requirements*.txt`, `package.json`, ...) and data files (`.csv`, `.parquet`,
  `.jsonl`, ...);
- what a protected path that is a committed symbolic link leads to inside the repository:
  with `.pre-commit-config.yaml -> src/ci/hooks.yaml`, the file `src/ci/hooks.yaml`, and with
  `.husky -> src/hooks`, everything under `src/hooks/` (the links on the way included).

Case does not matter (names are folded at least as Windows and macOS fold them, so
`requirementſ.txt`, with a long s, is `requirements.txt`), and neither do trailing dots and
spaces, `..` components or a symbolic link in the repository that leads to one of these paths.
Right before the commit, dag-healer checks every path of the final change again (the index, the
working tree and the untracked files, both sides of a rename), and that the clone's history has
not moved. After the commit, it checks the commit itself, which is what it pushes: the commit's
only parent must be the commit the branch started from, it may touch none of these paths, add
or change no symbolic link and no submodule or embedded repository, and stay within the diff
limits, and its stored message must hold the marker while `github.skip_ci` is on. A match ends
the repair `repair_failed`, with nothing pushed.

The fix may write only under `repo.allowed_paths` (default `src/` and `dags/`), and the test only
in the tests folder. An `allowed_paths` entry that names a protected path, such as `.github/`,
`.circleci` or the contracts folder of `contracts.dir`, is a configuration error. `.` stays
allowed, and the list above still applies inside it.
