from uuid import uuid4
import logging
import json
from py2neo.ogm import GraphObject
from py2neo.database import ClientError

from graphio.objects.relationship import Relationship
from graphio import defaults
from graphio.queries import query_create_rels_unwind, query_merge_rels_unwind
from graphio.queries.query_parameters import params_create_rels_unwind_from_objects
from graphio.objects.helper import chunks, create_single_index

log = logging.getLogger(__name__)


class RelationshipSet(GraphObject):
    """
    Container for a set of Relationships with the same type of start and end nodes.
    """

    def __init__(self, rel_type, start_node_labels, end_node_labels, start_node_properties, end_node_properties,
                 batch_size=None):
        """

        :param rel_type: Realtionship type.
        :type rel_type: str
        :param start_node_labels: Labels of the start node.
        :type start_node_labels: list[str]
        :param end_node_labels: Labels of the end node.
        :type end_node_labels: list[str]
        :param start_node_properties: Property keys to identify the start node.
        :type start_node_properties: list[str]
        :param end_node_properties: Properties to identify the end node.
        :type end_node_properties: list[str]
        :param batch_size: Batch size for Neo4j operations.
        :type batch_size: int
        """

        self.rel_type = rel_type
        self.start_node_labels = start_node_labels
        self.end_node_labels = end_node_labels
        self.start_node_properties = start_node_properties
        self.end_node_properties = end_node_properties

        self.uuid = str(uuid4())
        self.combined = '{0}_{1}_{2}_{3}_{4}'.format(self.rel_type,
                                                     '_'.join(sorted(self.start_node_labels)),
                                                     '_'.join(sorted(self.end_node_labels)),
                                                     '_'.join(sorted(self.start_node_properties)),
                                                     '_'.join(sorted(self.end_node_properties))
                                                     )

        if batch_size:
            self.batch_size = batch_size
        else:
            self.batch_size = defaults.BATCHSIZE

        self.relationships = []

        self.unique = False
        self.unique_rels = set()

    def add_relationship(self, start_node_properties, end_node_properties, properties):
        """
        Add a relationship to this RelationshipSet.

        :param properties: Relationship properties.
        """
        if self.unique:
            # construct a check set with start_node_properties (values), end_node_properties (values) and properties (values)
            check_set = frozenset(
                list(start_node_properties.values()) + list(end_node_properties.values()) + list(properties.values()))

            if check_set not in self.unique_rels:
                rel = Relationship(self.start_node_labels, self.end_node_labels, start_node_properties,
                                   end_node_properties, properties)
                self.relationships.append(rel)
                self.unique_rels.add(check_set)
        else:
            rel = Relationship(self.start_node_labels, self.end_node_labels, start_node_properties,
                               end_node_properties, properties)
            self.relationships.append(rel)

    def to_dict(self):
        for rel in self.relationships:
            yield rel.to_dict()

    def filter_relationships_target_node(self, filter_func):
        """
        Filter properties of target node with a filter function, remove relationships that do not match from main list.
        """
        filtered_rels = []
        discarded_rels = []

        for rel in self.relationships:

            if filter_func(rel.end_node_properties):
                filtered_rels.append(rel)
            else:
                discarded_rels.append(rel)

        self.relationships = filtered_rels
        self.discarded_nodes = discarded_rels

    def filter_relationships_start_node(self, filter_func):
        """
        Filter properties of target node with a filter function, remove relationships that do not match from main list.
        """
        filtered_rels = []
        discarded_rels = []

        for rel in self.relationships:

            if filter_func(rel.start_node_properties):
                filtered_rels.append(rel)
            else:
                discarded_rels.append(rel)

        self.relationships = filtered_rels
        self.discarded_nodes = discarded_rels

    def check_if_rel_exists(self, start_node_properties, end_node_properties, properties):
        for rel in self.relationships:
            if rel.start_node_properties == start_node_properties and rel.end_node_properties == end_node_properties and rel.properties == properties:
                return True

    def create(self, graph, batch_size=None):
        """
        Create relationships in this RelationshipSet
        """
        log.debug('Create RelationshipSet')
        if not batch_size:
            batch_size = self.batch_size
        log.debug('Batch Size: {}'.format(batch_size))

        # get query
        query = query_create_rels_unwind(self.start_node_labels, self.end_node_labels, self.start_node_properties,
                                         self.end_node_properties, self.rel_type)
        log.debug(query)

        i = 1
        # iterate over chunks of rels
        for batch in chunks(self.relationships, size=batch_size):
            batch = list(batch)
            log.debug('Batch {}'.format(i))
            log.debug(batch[0])
            # get parameters
            query_parameters = params_create_rels_unwind_from_objects(batch)
            log.debug(json.dumps(query_parameters))
            result = graph.run(query, **query_parameters)
            for r in result:
                print(r)
            i += 1

    def merge(self, graph, batch_size=None):
        """
        Create relationships in this RelationshipSet
        """
        log.debug('Create RelationshipSet')
        if not batch_size:
            batch_size = self.batch_size
        log.debug('Batch Size: {}'.format(batch_size))

        # get query
        query = query_merge_rels_unwind(self.start_node_labels, self.end_node_labels, self.start_node_properties,
                                        self.end_node_properties, self.rel_type)
        log.debug(query)

        i = 1
        # iterate over chunks of rels
        for batch in chunks(self.relationships, size=batch_size):
            batch = list(batch)
            log.debug('Batch {}'.format(i))
            log.debug(batch[0])
            # get parameters
            query_parameters = params_create_rels_unwind_from_objects(batch)
            log.debug(json.dumps(query_parameters))
            result = graph.run(query, **query_parameters)
            for r in result:
                print(r)
            i += 1

    def create_index(self, graph):
        """
        Create indices for start node and end node definition of this relationshipset. If more than one start or end
        node property is defined, all single property indices as well as the composite index are created.

        In Neo4j 3.x recreation of an index did not raise an error. In Neo4j 4 you cannot create an existing index.

        Index creation syntax changed from Neo4j 3.5 to 4. So far the old syntax is still supported. All py2neo
        functions (v4.4) work on both versions.
        """

        # from start nodes
        for label in self.start_node_labels:
            # create individual indexes
            for prop in self.start_node_properties:
                create_single_index(graph, label, prop)

            # composite indexes
            if len(self.start_node_properties) > 1:
                try:
                    graph.schema.create_index(label, *self.start_node_properties)
                except ClientError:
                    log.info("Index {}, {} cannot be created, it likely exists alredy.".format(label,
                                                                                               self.start_node_properties))
        for label in self.end_node_labels:
            for prop in self.end_node_properties:
                create_single_index(graph, label, prop)
                
            # composite indexes
            if len(self.end_node_properties) > 1:
                try:
                    graph.schema.create_index(label, *self.end_node_properties)
                except ClientError:
                    log.info("Index {}, {} cannot be created, it likely exists alredy.".format(label,
                                                                                               self.end_node_properties))
