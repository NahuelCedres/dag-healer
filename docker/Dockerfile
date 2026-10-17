# dag-healer development image: Apache Airflow 3.3.2 with the dag-healer wheel installed.
#
# The build context is the REPOSITORY ROOT (not docker/):
#
#   docker build -f docker/Dockerfile -t dag-healer/airflow:dev .
#   docker compose -f docker/compose.yaml build            # same thing, used by scripts/tasks.py
#
# Export only the compiled UI bundle (no Python involved):
#
#   docker build -f docker/Dockerfile --target ui-export --output type=local,dest=ui/dist .
#
# Stages:
#   ui         node:22-alpine compiles ui/ (Vite 8 needs Node ^20.19 || >=22.12)
#   ui-export  scratch stage that only holds ui/dist (for --output type=local)
#   wheel      python:3.12-slim builds the dag-healer wheel with the fresh bundle inside
#   (final)    apache/airflow:3.3.2 + the wheel (+ the demo repo requirements, + pytest)
#
# Requires BuildKit (default builder of Docker Desktop and Docker Engine >= 23).

ARG AIRFLOW_VERSION=3.3.2
ARG NODE_IMAGE=node:22-alpine
ARG PYTHON_IMAGE=python:3.12-slim

# --------------------------------------------------------------------------- 1. ui
FROM ${NODE_IMAGE} AS ui
# Non-interactive corepack/pnpm: never prompt before downloading the pinned pnpm.
ENV COREPACK_ENABLE_DOWNLOAD_PROMPT=0 \
    CI=true
WORKDIR /ui
RUN corepack enable
# Dependencies first so that source edits do not invalidate the pnpm install layer.
COPY ui/package.json ui/pnpm-lock.yaml ./
RUN pnpm install --frozen-lockfile
COPY ui/ ./
RUN pnpm build && ls -la dist

# --------------------------------------------------------------------------- 2. ui-export
FROM scratch AS ui-export
COPY --from=ui /ui/dist/ /

# --------------------------------------------------------------------------- 3. wheel
FROM ${PYTHON_IMAGE} AS wheel
ENV PIP_DISABLE_PIP_VERSION_CHECK=1 \
    PIP_NO_CACHE_DIR=1 \
    PIP_ROOT_USER_ACTION=ignore
WORKDIR /build
# README.md and LICENSE are matched with a one-character class so the COPY does not fail
# while one of them is still missing (BuildKit accepts wildcards that match nothing).
COPY pyproject.toml hatch_build.py README.m[d] LICENS[E] ./
COPY src/ ./src/
COPY --from=ui /ui/dist/ /tmp/ui-dist/
RUN set -eu; \
    bundle="$(ls -1 /tmp/ui-dist/*.umd.cjs 2>/dev/null | head -n 1 || true)"; \
    if [ -z "${bundle}" ]; then \
      echo "ERROR: the ui stage produced no *.umd.cjs bundle in ui/dist:" >&2; \
      ls -la /tmp/ui-dist >&2; \
      exit 1; \
    fi; \
    mkdir -p src/dag_healer/api/static; \
    cp "${bundle}" src/dag_healer/api/static/main.umd.cjs; \
    echo "UI bundle: ${bundle} -> src/dag_healer/api/static/main.umd.cjs"; \
    python -m pip wheel --no-deps -w /dist .; \
    ls -la /dist

# --------------------------------------------------------------------------- 4. airflow
FROM apache/airflow:${AIRFLOW_VERSION} AS airflow
# Requirements file of the user's repository, relative to the build context. It is
# installed together with dag-healer when it exists (the demo needs the example repo's
# libraries inside the image because its DAGs and its pytest suite run here).
ARG EXTRA_REQUIREMENTS=example_existing_repo/requirements.txt
# pytest is needed by the "tests" compose profile and by the healer worker, which runs
# the repository's test-suite while it repairs CODE failures.
ARG INSTALL_TEST_TOOLS=true

LABEL org.opencontainers.image.title="dag-healer/airflow" \
      org.opencontainers.image.description="Apache Airflow with the dag-healer plugin (development image)" \
      org.opencontainers.image.licenses="Apache-2.0"

USER root
# git ships with apache/airflow >= 3.0.2; install it only if a base image lacks it.
RUN set -eu; \
    if ! command -v git >/dev/null 2>&1; then \
      apt-get update; \
      apt-get install -y --no-install-recommends git; \
      apt-get autoremove -yqq --purge; \
      apt-get clean; \
      rm -rf /var/lib/apt/lists/*; \
    fi; \
    mkdir -p /opt/airflow/repo /opt/airflow/healer /opt/dag-healer/tests/airflow; \
    chown -R airflow:0 /opt/airflow/repo /opt/airflow/healer /opt/dag-healer; \
    chmod -R g+rwX /opt/airflow/repo /opt/airflow/healer /opt/dag-healer
USER airflow

COPY --from=wheel /dist/*.whl /tmp/wheels/
# One pip call so that the resolver sees everything at once; apache-airflow is pinned to
# the base image version so pip never upgrades or downgrades Airflow itself.
RUN --mount=type=bind,source=.,target=/tmp/build-context \
    set -eu; \
    extra=""; \
    if [ -n "${EXTRA_REQUIREMENTS}" ] && [ -f "/tmp/build-context/${EXTRA_REQUIREMENTS}" ]; then \
      echo "Installing extra requirements from ${EXTRA_REQUIREMENTS}"; \
      extra="-r /tmp/build-context/${EXTRA_REQUIREMENTS}"; \
    fi; \
    test_tools=""; \
    if [ "${INSTALL_TEST_TOOLS}" = "true" ]; then test_tools="pytest"; fi; \
    pip install --no-cache-dir "apache-airflow==${AIRFLOW_VERSION}" /tmp/wheels/*.whl ${test_tools} ${extra}; \
    pip check || echo "WARNING: pip check reported dependency conflicts (see above)"; \
    python -c "from dag_healer._version import __version__; print('dag-healer', __version__, 'installed')"

# Tests for the "tests" compose profile (compose also bind-mounts ../tests over this copy so
# that edits do not need a rebuild). The one-character class keeps the build working while
# tests/airflow does not exist yet.
COPY --chown=airflow:0 tests/airflo[w] /opt/dag-healer/tests/airflow/

WORKDIR /opt/airflow
