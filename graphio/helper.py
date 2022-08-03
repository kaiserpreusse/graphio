from itertools import chain, islice
import logging
from py2neo import ClientError

from graphio.graph import run_query_return_results

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
