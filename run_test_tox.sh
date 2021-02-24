#!/usr/bin/env bash
docker-compose -f test_neo4j_compose.yml up
tox --recreate
docker-compose -f test_neo4j_compose.yml down