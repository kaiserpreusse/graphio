from uuid import uuid4
import logging
import json
import os
from typing import Set, List

from graphio.helper import chunks, create_single_index, create_composite_index, run_query_return_results
from graphio.queries import rels_create_factory, rels_merge_factory, rels_params_from_objects
from graphio.config import config

log = logging.getLogger(__name__)


def tuplify_json_list(list_object: list) -> tuple:
    """
    JSON.dump() stores tuples as JSON lists. This function receives a list (with sub lists)
    and creates a tuple of tuples from the list.

    E.g.

        [[a, b], [c, d]] -> ((a, b), (c, d))

    :param list_object: A list with sub-lists.
    :return: A tuple version of the list object.
    """
    output = tuple()
    for element in list_object:
        if isinstance(element, list):
            output = output + (tuple(element),)
        else:
            output = output + (element,)
    return output


class RelationshipSet:
    """
    Container for a set of Relationships with the same type of start and end nodes.
    """

    def __init__(self, rel_type: str, start_node_labels: List[str], end_node_labels: List[str],
                 start_node_properties: List[str], end_node_properties: List[str],
                 default_props: dict = None):
        """

        :param rel_type: Realtionship type.
        :param start_node_labels: Labels of the start node.
        :param end_node_labels: Labels of the end node.
        :param start_node_properties: Property keys to identify the start node.
        :param end_node_properties: Properties to identify the end node.
        """

        self.rel_type = rel_type
        self.start_node_labels = start_node_labels
        self.end_node_labels = end_node_labels
        if not self.start_node_labels:
            log.warning("Creating or merging relationships without start node labels is slow because no index is used.")
        if not self.end_node_labels:
            log.warning("Creating or merging relationships without end node labels is slow because no index is used.")
        self.start_node_properties = start_node_properties
        self.end_node_properties = end_node_properties
        self.default_props = default_props

        self.fixed_order_start_node_properties = tuple(self.start_node_properties)
        self.fixed_order_end_node_properties = tuple(self.end_node_properties)

        self.uuid = str(uuid4())
        self.combined = '{0}_{1}_{2}_{3}_{4}'.format(self.rel_type,
                                                     '_'.join(sorted(self.start_node_labels)),
                                                     '_'.join(sorted(self.end_node_labels)),
                                                     '_'.join(sorted([str(x) for x in self.start_node_properties])),
                                                     '_'.join(sorted([str(x) for x in self.end_node_properties]))
                                                     )

        self.relationships = []

        self.unique = False
        self.unique_rels = set()

    def __str__(self):
        return f"<RelationshipSet ({self.start_node_labels}; {self.start_node_properties})-[{self.rel_type}]->({self.end_node_labels}; {self.end_node_properties})>"

    def add_relationship(self, start_node_properties: dict, end_node_properties: dict, properties: dict = None):
        """
        Add a relationship to this RelationshipSet.

        :param properties: Relationship properties.
        """
        if not properties:
            properties = {}
        if self.default_props:
            rel_props = {**self.default_props, **properties}
        else:
            rel_props = properties

        if self.unique:
            # construct a check set with start_node_properties (values), end_node_properties (values) and properties (values)
            check_set = frozenset(
                list(start_node_properties.values()) + list(end_node_properties.values()) + list(rel_props.values()))

            if check_set not in self.unique_rels:
                self.relationships.append((start_node_properties, end_node_properties, rel_props))
                self.unique_rels.add(check_set)
        else:
            self.relationships.append((start_node_properties, end_node_properties, rel_props))

    def all_property_keys(self) -> Set[str]:
        """
        Return a set of all property keys in this RelationshipSet

        :return: A set of unique property keys of a NodeSet
        """
        all_props = set()

        # collect properties
        for r in self.relationships:
            all_props.update(r[2].keys())

        return all_props

    @property
    def metadata_dict(self):
        return {"rel_type": self.rel_type,
                "start_node_labels": self.start_node_labels,
                "end_node_labels": self.end_node_labels,
                "start_node_properties": self.start_node_properties,
                "end_node_properties": self.end_node_properties}

    def object_file_name(self, suffix: str = None) -> str:
        """
        Create a unique name for this RelationshipSet that indicates content. Pass an optional suffix.
        NOTE: suffix has to include the '.' for a filename!

            `relationshipset_StartLabel_TYPE_EndLabel_uuid`

        With suffix:

            `relationshipset_StartLabel_TYPE_EndLabel_uuid.json`
        """
        basename = f"relationshipset_{'_'.join(self.start_node_labels)}_{self.rel_type}_{'_'.join(self.end_node_labels)}_{self.uuid}"
        if suffix:
            basename += suffix
        return basename

    def create(self, graph, database=None, batch_size=None):
        """
        Create relationships in this RelationshipSet
        """
        log.debug('Create RelationshipSet')
        if not batch_size:
            batch_size = config.BATCHSIZE

        # iterate over chunks of rels
        q = rels_create_factory(self.start_node_labels, self.end_node_labels, self.start_node_properties,
                                self.end_node_properties, self.rel_type)

        # Define transaction function once
        def create_batch(tx, batch_params):
            result = tx.run(q, **batch_params)
            result.consume()
            return []

        with graph.session(database=database) as session:
            for batch in chunks(self.relationships, size=batch_size):
                query_parameters = rels_params_from_objects(batch)
                session.execute_write(create_batch, query_parameters)

    def merge(self, graph, database=None, batch_size=None):
        """
        Create relationships in this RelationshipSet
        """
        if not batch_size:
            batch_size = config.BATCHSIZE
        log.debug('Batch Size: {}'.format(batch_size))

        # iterate over chunks of rels
        q = rels_merge_factory(self.start_node_labels, self.end_node_labels, self.start_node_properties,
                               self.end_node_properties, self.rel_type)

        # Define transaction function once
        def merge_batch(tx, batch_params):
            result = tx.run(q, **batch_params)
            result.consume()
            return []

        with graph.session(database=database) as session:
            for batch in chunks(self.relationships, size=batch_size):
                query_parameters = rels_params_from_objects(batch)
                session.execute_write(merge_batch, query_parameters)

    def create_index(self, graph, database=None):
        """
        Create indices for start node and end node definition of this relationshipset. If more than one start or end
        node property is defined, all single property indices as well as the composite index are created.
        """

        # from start nodes
        for label in self.start_node_labels:
            # create individual indexes
            for prop in self.start_node_properties:
                create_single_index(graph, label, prop, database=database)

            # composite indexes
            if len(self.start_node_properties) > 1:
                create_composite_index(graph, label, self.start_node_properties, database=database)

        for label in self.end_node_labels:
            for prop in self.end_node_properties:
                create_single_index(graph, label, prop, database=database)

            # composite indexes
            if len(self.end_node_properties) > 1:
                create_composite_index(graph, label, self.end_node_properties, database=database)
