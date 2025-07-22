
help:
	@echo "make localdb - run DB only in background"
	@echo "make stop - stop all containers"
	@echo "make test - run tests"
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

.PHONY: help localdb stop test lint format fix check docs