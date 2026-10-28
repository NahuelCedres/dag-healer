# Releasing dag-healer

This page is for the maintainers who publish dag-healer to PyPI. The Release workflow
([`.github/workflows/release.yml`](../.github/workflows/release.yml)) builds the package and
uploads it when a tag `v<version>` is pushed. It signs in to PyPI with
[trusted publishing](https://docs.pypi.org/trusted-publishers/): PyPI accepts an OpenID Connect
(OIDC) token that GitHub issues to that workflow, so no PyPI API token or password is stored in the
repository or in its settings. Every upload waits for a maintainer's approval.

## What the workflow does

The workflow starts only when a tag that begins with `v` is pushed. It never runs for a branch,
`healer/...` branches included, or for a pull request. It has three jobs:

1. **Build the distributions**, with read access to the repository's contents and nothing more:
   - it checks that the tag is `v` followed by the `__version__` of `src/dag_healer/_version.py`
     at the tagged commit (`v0.2.0` for `__version__ = "0.2.0"`), and stops otherwise;
   - it builds both UI bundles the way CI's "UI bundle and wheel" job does (Node.js 22, the pnpm
     version of `ui/package.json` and `python scripts/tasks.py ui --local`), over the committed
     bundles in `src/dag_healer/api/static/`. When the two differ it warns, and the release ships
     the fresh build;
   - it builds the sdist and the wheel with `python -m build`, checks that `dist/` holds exactly
     `dag_healer-<version>.tar.gz` and `dag_healer-<version>-py3-none-any.whl`, checks that the
     wheel ships both bundles byte for byte, and runs `twine check --strict`;
   - it keeps both files as the run's `dist` artifact.
2. **Publish to TestPyPI**, in the `testpypi` environment: it waits for an approval, then uploads
   the `dist` files to test.pypi.org. A file that TestPyPI already has, from an earlier run of the
   same version, is skipped.
3. **Publish to PyPI**, in the `pypi` environment, only once the TestPyPI job has succeeded: it
   waits for an approval, then uploads the same files to pypi.org.

The publish jobs have the `id-token: write` permission and nothing more: their token can ask GitHub
for the OIDC token but cannot change anything in the repository, and they never check it out.
[pypa/gh-action-pypi-publish](https://github.com/pypa/gh-action-pypi-publish) exchanges the token
for a short-lived upload token and uploads a signed attestation (PEP 740) with each file, which
PyPI shows as the file's provenance. Every action in the workflow is pinned to a full commit SHA.

## One-time setup

Do this once, before the first release. Create the GitHub environments before you push any `v`
tag: a run that names an environment that does not exist creates it without protection rules, and
its upload would not wait for anyone.

### The environments on GitHub

As an administrator of the repository, open Settings > Environments > New environment, name it
`pypi` and click Configure environment. Then set:

- **Required reviewers:** select it and add the maintainers who may approve a release (up to 6
  people or teams; one approval is enough). Leave **Prevent self-review** off if you release
  alone: with it on, the person who pushed the tag cannot approve the run.
- **Allow administrators to bypass configured protection rules:** clear it, so that every upload
  waits for an approval, including an administrator's.
- **Deployment branches:** choose Selected branches and tags, click Add deployment branch or tag
  rule, choose the ref type Tag, enter the name pattern `v*` and click Add rule. Jobs that run for
  any other ref cannot use the environment.
- No environment secrets and no environment variables: trusted publishing needs none.

Click Save protection rules. Then create the `testpypi` environment the same way.

### Pending trusted publishers on PyPI and TestPyPI

Until the first release, `dag-healer` does not exist on PyPI, so the trusted publisher is a
*pending* one: PyPI creates the project with the first upload from the workflow. Registering it
needs a PyPI account with two-factor authentication and a verified email address.

On PyPI, open Your account > Publishing (<https://pypi.org/manage/account/publishing/>). Under "Add
a new pending publisher", choose GitHub, fill in these values and add the publisher:

| Field | Value |
|---|---|
| PyPI Project Name | `dag-healer` |
| Owner | the account that owns this repository on GitHub: `<owner>` in `github.com/<owner>/dag-healer` |
| Repository name | `dag-healer` |
| Workflow name | `release.yml` |
| Environment name | `pypi` |

Do the same on TestPyPI (<https://test.pypi.org/manage/account/publishing/>), with the environment
name `testpypi`. TestPyPI is a separate site with its own accounts: register there first if you
have no account.

- The workflow name is the file name alone, and the environment name must be exactly the job's
  `environment`. PyPI then accepts uploads only from a job that ran in that environment, so only
  after a reviewer approved it.
- A pending publisher does not reserve the name. Until the first upload creates the project, anyone
  can still register `dag-healer`, and the pending publisher then stops working. Make the first
  release soon after this setup.
- Once the project exists, its publisher is listed under the project's Manage > Publishing. Change
  it there if the repository, the workflow file or an environment is renamed, or the uploads fail
  with `invalid-publisher`.

### Recommended: protect the release tags

Anyone who can push to the repository can push a tag. A tag publishes nothing without an approval,
but a tag ruleset also keeps others from creating, moving or deleting release tags. Open Settings >
Rules > Rulesets > New ruleset > New tag ruleset, name it (for example "Release tags"), set the
enforcement status to Active, add the target Include by pattern `v*`, select Restrict creations,
Restrict updates and Restrict deletions, and add the Repository admin role to the bypass list so
that you can still create release tags.

## Releasing a version

The examples release `0.2.0`. For the first release, `__version__` is already `0.1.0`: skip step 2
and tag the current `main`.

1. Check that `main` holds everything for the release and that CI passed on its last commit. The
   Release workflow builds and checks the package, but does not run the tests.
2. Set the new version in `src/dag_healer/_version.py`, in its normalized form
   ([PEP 440](https://packaging.python.org/en/latest/specifications/version-specifiers/)):
   `0.2.0`, or `0.2.0rc1` for a release candidate, never `v0.2.0` or `0.2.0-rc1`. Commit it to
   `main` with a Conventional Commit:

   ```sh
   git switch main
   git pull --ff-only
   # edit src/dag_healer/_version.py: __version__ = "0.2.0"
   git add src/dag_healer/_version.py
   git commit -m "build: release 0.2.0"
   git push origin main
   ```

   If `main` only accepts pull requests, merge the change through one with the same title.
3. Tag that commit and push the tag, and only that tag:

   ```sh
   git switch main
   git pull --ff-only
   git log -1 --oneline   # the commit "build: release 0.2.0"
   git tag -a v0.2.0 -m "dag-healer 0.2.0"
   git push origin v0.2.0
   ```

4. Open the run under Actions > Release. Once "Build the distributions" has passed, "Publish to
   TestPyPI" waits for you: click Review deployments, select `testpypi` and click Approve and
   deploy.
5. Check the release on TestPyPI, at <https://test.pypi.org/project/dag-healer/0.2.0/>: the version,
   the description and the two files. To try the package before it reaches PyPI, download the
   `dist` artifact from the run's summary page (or with `gh run download <run id> --name dist`).
   It holds the files that the PyPI job uploads. Install the wheel in a new virtual environment:

   ```sh
   python -m venv /tmp/dag-healer-0.2.0
   /tmp/dag-healer-0.2.0/bin/python -m pip install dag_healer-0.2.0-py3-none-any.whl
   /tmp/dag-healer-0.2.0/bin/dag-healer version
   ```

   Do not install dag-healer from TestPyPI with `--extra-index-url`: pip would take its
   dependencies from TestPyPI as well, where anyone can upload a package under their names.
6. Approve "Publish to PyPI" the same way, for `pypi`. To stop the release instead, click Reject:
   nothing reaches PyPI.
7. Check the release on PyPI, at <https://pypi.org/project/dag-healer/0.2.0/>: the version, the
   description, the two files and, in the details of each file (Download files, then View
   details), its provenance: the workflow `release.yml` of this repository, run for the tag
   `v0.2.0`. Then install it in a new virtual environment (pip can take a few minutes to see a new
   release):

   ```sh
   python -m venv /tmp/dag-healer-pypi
   /tmp/dag-healer-pypi/bin/python -m pip install dag-healer==0.2.0
   /tmp/dag-healer-pypi/bin/dag-healer version
   ```

   It prints `dag-healer 0.2.0`.
8. Optionally, create a GitHub release for the tag, with notes generated from the pull requests:

   ```sh
   gh release create v0.2.0 --verify-tag --generate-notes
   ```

## When something goes wrong

- **The build job fails**, for example because the tag does not match `__version__`: nothing was
  uploaded. Delete the tag on GitHub and locally, fix the cause on `main`, then tag again (step 3):

  ```sh
  git push origin --delete refs/tags/v0.2.0
  git tag -d v0.2.0
  ```

- **A publish job fails with `invalid-publisher`**, or another trusted publishing error: the
  trusted publisher does not match the run. The job's log shows the claims of the token that PyPI
  received (repository, workflow, environment, ref). Correct the publisher on PyPI or TestPyPI,
  then click Re-run jobs > Re-run failed jobs on the run's page. The re-run uploads the files of the
  first build, after a new approval.
- **TestPyPI or PyPI is down:** re-run the failed jobs later. The PyPI job does not start until the
  TestPyPI job has succeeded.
- **You find a problem before you approve the PyPI job:** click Reject. PyPI has nothing yet, so the
  version can be reused: fix the cause on `main`, delete the tag as above and tag the fixed commit
  (step 3). TestPyPI keeps the files of the first run, and the new run skips them there, so try the
  new files from the new run's `dist` artifact (step 5).
- **A published release is broken:** PyPI never accepts the same file name twice, even after a
  deletion, so a version cannot be replaced. Release the fix as the next version (`0.2.1`) and yank
  the broken one (the project's Manage > Releases, then Options > Yank next to the version): pip
  then installs it only when asked for that exact version. Do not delete it, which breaks the users
  who pinned it.

## Keeping the workflow current

- Every action in `release.yml` is pinned to the full commit SHA of a release, with the release in a
  comment. To update one, read its release notes, take the commit of the new release's tag from the
  action's own repository, and change the SHA and the comment together:

  ```sh
  gh api repos/pypa/gh-action-pypi-publish/commits/v1.14.3 --jq .sha
  ```

- `build` and `twine` are pinned in the step "Install the build tools": update them by hand.
  `hatchling` is not pinned there: `python -m build` installs the newest release that
  `build-system.requires` in `pyproject.toml` allows. If `twine check` rejects the metadata of a
  newer hatchling, update `twine`.
- The UI build and the wheel check are copies of the "UI bundle and wheel" job of `ci.yml`: change
  both workflows together.
