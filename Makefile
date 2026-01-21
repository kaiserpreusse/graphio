
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
	@for py in 3.10 3.11 3.12 3.13 3.14; do \
		echo "=== Testing Python $$py ===" && \
		uv run --python $$py --isolated pytest || exit 1; \
	done

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

.PHONY: help localdb stop test test-all lint format fix check docs