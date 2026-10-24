# Tiny image for the opt-in real-engine tests of the test sandbox (tests/unit/test_sandbox_engine.py).
#
#   docker build -t dag-healer/sandbox-test:dev tests/sandbox
#   HEALER_SANDBOX_TEST_IMAGE=dag-healer/sandbox-test:dev \
#       python -m pytest -q -p no:cacheprovider tests/unit/test_sandbox_engine.py
#
# Like the Docker demo stack's image, it runs as a non-root USER with pytest under that user's
# home (~/.local). Here pytest is in the user site-packages (pip install --user), so the tests
# also prove that the sandbox keeps them importable when it moves HOME to its tmpfs. It holds
# no secret. The sandbox replaces the entrypoint with the interpreter, so none is set here.
FROM python:3.12-slim

RUN useradd --create-home --uid 10001 --shell /usr/sbin/nologin sandbox
USER sandbox
RUN python -m pip install --user --no-cache-dir --disable-pip-version-check --no-warn-script-location \
        "pytest==9.1.1"
