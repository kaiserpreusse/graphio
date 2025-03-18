from itertools import chain, islice
import logging
import datetime

from neo4j import Driver, DEFAULT_DATABASE
from neo4j.time import DateTime as Neo4jDateTime, Date as Neo4jDate


log = logging.getLogger(__name__)


def chunks(iterable, size=10):
    """
    Get chunks of an iterable without pre-walking it.

    https://stackoverflow.com/questions/24527006/split-a-generator-into-chunks-without-pre-walking-it

    :param iterable: The iterable.
    :param size: Chunksize.
    :return: Yield chunks of defined size.
    """
    iterator = iter(iterable)
    for first in iterator:
        yield chain([first], islice(iterator, int(size) - 1))


def create_single_index(graph, label, prop, database=None):
    """
    Create an inidex on a single property.

    :param label: The label.
    :param prop: The property.
    """

    log.debug("Create index {}, {}".format(label, prop))
    q = f"CREATE INDEX IF NOT EXISTS FOR (n:{label}) ON (n.{prop})"
    log.debug(q)
    run_query_return_results(graph, q, database=database)


def create_composite_index(graph, label, properties, database=None):
    """
    Create an inidex on a single property.

    :param label: The label.
    :param prop: The property.
    """

    property_list = []
    for prop in properties:
        property_list.append(f"n.{prop}")

    q = f"CREATE INDEX IF NOT EXISTS FOR (n:{label}) ON ({','.join(property_list)})"
    log.debug(q)
    run_query_return_results(graph, q, database=database)


def run_query_return_results(connection: Driver, query: str, database: str = None, **params):
    if not database:
        database = DEFAULT_DATABASE
    with connection.session(database=database) as s:
        result = list(s.run(query, **params))

    return result


def convert_neo4j_types_to_python(data):
    """Convert Neo4j specific types to Python standard types."""
    if isinstance(data, dict):
        return {k: convert_neo4j_types_to_python(v) for k, v in data.items()}
    elif isinstance(data, list):
        return [convert_neo4j_types_to_python(item) for item in data]
    elif isinstance(data, Neo4jDateTime):
        # Convert Neo4j DateTime to Python datetime
        return datetime.datetime(
            year=data.year,
            month=data.month,
            day=data.day,
            hour=data.hour,
            minute=data.minute,
            second=data.second,
            microsecond=data.nanosecond // 1000,
            tzinfo=datetime.timezone.utc if data.tzinfo else None
        )
    elif isinstance(data, Neo4jDate):
        # Convert Neo4j Date to Python date
        return datetime.date(
            year=data.year,
            month=data.month,
            day=data.day
        )

    return data
