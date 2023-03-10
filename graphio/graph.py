from neo4j import GraphDatabase, Driver, DEFAULT_DATABASE
from typing import Union


def run_query_return_results(connection: Driver, query: str, database: str = None, **params):
    if not database:
        database = DEFAULT_DATABASE
    with connection.session(database=database) as s:
        result = list(s.run(query, **params))

    return result
