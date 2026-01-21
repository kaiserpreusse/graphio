# GraphIO

[![Tests](https://github.com/kaiserpreusse/graphio/actions/workflows/test.yml/badge.svg)](https://github.com/kaiserpreusse/graphio/actions/workflows/test.yml)
[![PyPI](https://img.shields.io/pypi/v/graphio)](https://pypi.org/project/graphio)
[![License](https://img.shields.io/badge/License-Apache%202.0-blue.svg)](https://opensource.org/licenses/Apache-2.0)
[![Neo4j](https://img.shields.io/badge/Neo4j-5-blue)](https://neo4j.com)
[![Python](https://img.shields.io/badge/Python-3.10%20%7C%203.11%20%7C%203.12%20%7C%203.13%20%7C%203.14-green)](https://python.org)
[![Downloads](https://pepy.tech/badge/graphio)](https://pepy.tech/project/graphio)

OGM and data loader for Neo4j with two main approaches:

- **OGM (Object Graph Mapper)**: Pydantic-based models with Neo4j integration for complex data models and applications
- **Datasets (NodeSet/RelationshipSet)**: Bulk data containers optimized for fast data loading and testing
- **Multi-Database Support**: Full support for Neo4j Enterprise Edition multi-database feature

## Documentation

Docs available at: https://graphio.readthedocs.io

## Quick Start

### Installation

Install graphio from PyPI:

```bash
pip install graphio
```

Install the latest version from GitHub:

```bash
pip install git+https://github.com/kaiserpreusse/graphio.git
```

### Example

```python
from graphio import NodeModel, Base
from neo4j import GraphDatabase

# Set up connection
driver = GraphDatabase.driver('neo4j://localhost:7687', auth=('neo4j', 'password'))
Base.set_driver(driver)
# Optional: Set target database (Enterprise Edition)
# Base.set_database('production')

# Define OGM model for structure and validation
class Person(NodeModel):
    _labels = ['Person']
    _merge_keys = ['email']
    name: str
    email: str

# Get bulk container directly from OGM model
people = Person.dataset()  # Automatically uses Person's labels and merge_keys

for person_data in large_dataset:
    # Create validated OGM instance and add directly
    person = Person(**person_data)  # Pydantic validation happens here
    people.add(person)  # Add validated instance to bulk dataset

people.create(driver)  # Bulk create with validation benefits

# Use OGM for application logic
alice = Person.match(Person.email == 'alice@example.com').first()
```

## Development

### Prerequisites

- Python 3.10+ 
- [uv](https://docs.astral.sh/uv/) (recommended) or pip
- Docker for running test databases

### Setup

```bash
# Clone the repository
git clone https://github.com/kaiserpreusse/graphio.git
cd graphio

# Install dependencies
uv sync --extra dev

# Start Neo4j test databases
make localdb
```

### Common Commands

```bash
# Run tests
make test

# Check code style  
make lint

# Format code
make format

# Fix linting issues and format
make fix

# Run all checks (lint + test)
make check

# Serve documentation locally
make docs

# See all available commands
make help
```

### Testing

The test suite requires Docker containers running Neo4j. Start them with:

```bash
make localdb
```

Then run tests with:

```bash
make test
# or directly:
uv run pytest
```

All tests using the `graph` fixture will run against both Neo4j Community and Enterprise editions.

## Feedback
Please provide feedback, ideas and bug reports through GitHub issues.


