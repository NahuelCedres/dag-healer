# Convenience wrapper: every target delegates to scripts/tasks.py, which also works
# without make (python scripts/tasks.py <target>). Extra arguments go in ARGS, e.g.
#   make test ARGS="-k config"      make logs SERVICE=airflow-scheduler
PY ?= python
TASKS := $(PY) scripts/tasks.py
ARGS ?=
SERVICE ?=

.PHONY: help up down build ui init test test-airflow demo demo-reset e2e logs ps doctor

help: ; @$(TASKS) --help
up: ; $(TASKS) up $(ARGS)
down: ; $(TASKS) down $(ARGS)
build: ; $(TASKS) build $(ARGS)
ui: ; $(TASKS) ui $(ARGS)
init: ; $(TASKS) init $(ARGS)
test: ; $(TASKS) test $(ARGS)
test-airflow: ; $(TASKS) test-airflow $(ARGS)
demo: ; $(TASKS) demo $(ARGS)
demo-reset: ; $(TASKS) reset --yes
e2e: ; $(TASKS) e2e $(ARGS)
logs: ; $(TASKS) logs --follow $(ARGS) $(SERVICE)
ps: ; $(TASKS) ps
doctor: ; $(TASKS) doctor $(ARGS)
