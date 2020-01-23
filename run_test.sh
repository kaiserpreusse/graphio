#!/usr/bin/env bash

# run neo4j container
docker run --name graphio_test_neo4j -p 7474:7474 -p 7473:7473 -p 7687:7687 --env NEO4J_AUTH=neo4j/test neo4j:3.5