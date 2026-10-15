
help:
	@echo "make localdb - run DB only in background"
	@echo "make stop - stop all containers"
	@echo "make test - run tests"
	@echo "make test-all - run tests against all Python versions (3.10-3.14)"
	@echo "make lint - check code style with ruff"
	@echo "make format - format code with ruff"
	@echo "make fix - fix code issues and format"
	@echo "make check - run all checks (lint + test)"
	@echo "make docs - serve documentation locally"
	@echo "make test-kweladb - experimental KwelaDB transport tests (needs a running KwelaDB)"
	@echo "make test-kweladb-suite - graphio's suite against KwelaDB, bulk loads over Flight"

# Database commands
localdb:
	docker compose down
	docker compose up -d

stop:
	docker compose down

# Development commands
test:
	uv run pytest

test-all:
	@unset VIRTUAL_ENV; \
	FAILED=""; \
	for py in 3.10 3.11 3.12 3.13 3.14; do \
		printf "\n=== Python $$py ===\n"; \
		UV_PROJECT_ENVIRONMENT=.venv-py$$py uv sync --python $$py --extra dev --quiet; \
		if UV_PROJECT_ENVIRONMENT=.venv-py$$py uv run --python $$py pytest -q; then \
			:; \
		else \
			FAILED="$$FAILED $$py"; \
		fi; \
	done; \
	printf "\n"; \
	if [ -z "$$FAILED" ]; then \
		echo "All Python versions passed."; \
	else \
		echo "FAILED on:$$FAILED"; \
		exit 1; \
	fi

lint:
	uv run ruff check graphio/

format:
	uv run ruff format graphio/

fix:
	uv run ruff check graphio/ --fix
	uv run ruff format graphio/

check: lint test
	@echo "All checks passed!"

# Documentation commands
docs:
	uv run mkdocs serve

.PHONY: help localdb stop test test-all lint format fix check docs test-kweladb test-kweladb-suite

# Experimental KwelaDB transport (graphio/kweladb.py): not part of `make test` or CI. Needs a
# KwelaDB started outside graphio; in the KwelaDB repository: make graphio-test-up
KWELA_BOLT ?= bolt://localhost:7690
KWELA_FLIGHT ?= grpc://localhost:8817

test-kweladb:
	KWELA_TEST_BOLT=$(KWELA_BOLT) KWELA_TEST_FLIGHT=$(KWELA_FLIGHT) \
		uv run --extra dev --extra kweladb pytest test_kweladb

# The batch size tests count UNWIND batches; over Flight a set goes as one table, not in batches.
test-kweladb-suite:
	GRAPHIO_TEST_TARGETS='[{"name": "kweladb", "uri": "$(KWELA_BOLT)", "user": "neo4j", "password": "test"}]' \
		GRAPHIO_KWELA_FLIGHT=$(KWELA_FLIGHT) GRAPHIO_KWELA_FLIGHT_MIN_ROWS=1 \
		uv run --extra dev --extra kweladb pytest test -q -W ignore::UserWarning -k 'not batch_size_exactness'
