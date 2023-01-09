from py2neo import Graph
from neo4j import GraphDatabase, Driver, DEFAULT_DATABASE
from typing import Union


def run_query_return_results(connection:Union[Graph, Driver], query: str, database: str = None, **params):

    if isinstance(connection, Graph):
        result = list(connection.run(query, **params))
        return result

    elif isinstance(connection, Driver):

        if not database:
            database = DEFAULT_DATABASE
        with connection.session(database=database) as s:
            result = list(s.run(query, **params))

        return result

