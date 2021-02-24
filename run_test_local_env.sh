#!/usr/bin/env bash
docker-compose -f test_neo4j_compose.yml up
python -m pytest
docker-compose -f test_neo4j_compose.yml up