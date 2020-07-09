#!/usr/bin/env bash
docker run -d -p 9474:7474 -p 9473:7473 -p 9687:7687 -e NEO4J_AUTH=neo4j/test --name neo4j_graphio_test_35 neo4j:3.5
docker run -d -p 8474:7474 -p 8473:7473 -p 8687:7687 -e NEO4J_AUTH=neo4j/test --name neo4j_graphio_test_40 neo4j:4.0
python -m pytest
docker stop neo4j_graphio_test_35
docker stop neo4j_graphio_test_40
docker rm neo4j_graphio_test_35
docker rm neo4j_graphio_test_40