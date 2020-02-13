# GraphIO

[![Build Status](https://travis-ci.com/kaiserpreusse/graphio.svg?branch=master)](https://travis-ci.com/kaiserpreusse/graphio)
[![PyPI](https://img.shields.io/pypi/v/graphio)](https://pypi.org/project/graphio)
[![License](https://img.shields.io/badge/License-Apache%202.0-blue.svg)](https://opensource.org/licenses/Apache-2.0)
[![Neo4j](https://img.shields.io/badge/Neo4j-3.4%20%7C%203.5%20%7C%204.0-blue)](https://neo4j.com)

A Python library to quickly build data sets and load them to Neo4j. Based on Py2neo. Built by [Kaiser & Preusse](https://kaiser-preusse.com).

## Documentation
[![Documentation Status](https://readthedocs.org/projects/graphio/badge/?version=latest)](https://graphio.readthedocs.io/en/latest/?badge=latest)

Docs available at: https://graphio.readthedocs.io/

Tutorial with real data: https://graphdb-bio.com/graphio-tutorial-idmapping

## Install
Install graphio from PyPI:

```shell script
pip install graphio
```

Install the latest build version from github:

```shell script
pip install git+https://github.com/kaiserpreusse/graphio.git
```

## Status
This is an early release with a focus on data loading.

## Development
You need Docker to run the test suite. Two Neo4j Docker containers will be started before 
running the tests, one for version 3.5 and another for version 4.

All tests that use the `graph` fixture found in `tests/conftest.py` will run against both databases.

## Feedback
Please provide feedback, ideas and bug reports through github issues.


