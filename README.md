# GraphIO

[![Tests](https://github.com/kaiserpreusse/graphio/actions/workflows/test.yml/badge.svg)](https://github.com/kaiserpreusse/graphio/actions/workflows/test.yml)
[![codecov](https://codecov.io/gh/kaiserpreusse/graphio/branch/master/graph/badge.svg?token=94HOVGB4J3)](https://codecov.io/gh/kaiserpreusse/graphio)
[![PyPI](https://img.shields.io/pypi/v/graphio)](https://pypi.org/project/graphio)
[![License](https://img.shields.io/badge/License-Apache%202.0-blue.svg)](https://opensource.org/licenses/Apache-2.0)
[![Neo4j](https://img.shields.io/badge/Neo4j-4.2%20%7C%204.3%20%7C%204.4%20%7C%205.1-blue)](https://neo4j.com)
[![Neo4j](https://img.shields.io/badge/Python-3.7%20%7C%203.8%20%7C%203.9%20%7C%203.10-green)](https://python.com)
[![Downloads](https://pepy.tech/badge/graphio)](https://pepy.tech/project/graphio)

A Python library to bulk load data to Neo4j.

## Documentation

Docs available at: https://graphio.readthedocs.io

## Install
Install graphio from PyPI:

```shell script
pip install graphio
```

Install the latest build version from github:

```shell script
pip install git+https://github.com/kaiserpreusse/graphio.git
```

## Development
You need Docker to run the test suite. First start the Neo4j instances to test against:

```shell
docker-compose -f test_neo4j_compose.yml up
```

Install dependencies:
```shell
pip install -r requirements.txt
pip install -r test_requirements.txt
```

Then run the tests:

```shell
python -m pytest
```

All tests that use the `graph` fixture found in `tests/conftest.py` will run against all databases.

## Feedback
Please provide feedback, ideas and bug reports through GitHub issues.


