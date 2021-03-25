from uuid import uuid4
import logging
import json
from py2neo.bulk import create_relationships, merge_relationships

from graphio import defaults
from graphio.objects.helper import chunks, create_single_index, create_composite_index

log = logging.getLogger(__name__)


class RelationshipSet:
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

        self.fixed_order_start_node_properties = tuple(self.start_node_properties)
        self.fixed_order_end_node_properties = tuple(self.end_node_properties)

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

    def __relationship_from_dictionary(self, start_node_properties, end_node_properties, properties):
        """
        Transform the input dictionary into the relationship data type used for py2neo.

        :return: Relationship data.
        """
        start_node_data = tuple(start_node_properties[x] for x in self.fixed_order_start_node_properties)
        if len(start_node_data) == 1:
            start_node_data = start_node_data[0]
        end_node_data = tuple(end_node_properties[x] for x in self.fixed_order_end_node_properties)
        if len(end_node_data) == 1:
            end_node_data = end_node_data[0]

        return (start_node_data, properties, end_node_data)

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
                self.relationships.append(self.__relationship_from_dictionary(start_node_properties, end_node_properties, properties))
                self.unique_rels.add(check_set)
        else:
            self.relationships.append(self.__relationship_from_dictionary(start_node_properties, end_node_properties, properties))


    def to_dict(self):
        return {"rel_type": self.rel_type,
                "start_node_labels": self.start_node_labels,
                "end_node_labels": self.end_node_labels,
                "start_node_properties": self.start_node_properties,
                "end_node_properties": self.end_node_properties,
                "unique": self.unique,
                "relationships": self.relationships}

    @classmethod
    def from_dict(cls, relationship_dict, batch_size=None):
        rs = cls(rel_type=relationship_dict["rel_type"],
                 start_node_labels=relationship_dict["start_node_labels"],
                 end_node_labels=relationship_dict["end_node_labels"],
                 start_node_properties=relationship_dict["start_node_properties"],
                 end_node_properties=relationship_dict["end_node_properties"],
                 batch_size=batch_size)
        rs.unique = relationship_dict["unique"]
        rs.relationships = relationship_dict["relationships"]

        return rs

    def create(self, graph, batch_size=None):
        """
        Create relationships in this RelationshipSet

        py2neo bulk works with tuples and th order of elements in the tuple. The underlying Relationship used in the
        RelationshipSet uses a dictionary. Work around this for now, adapt the RelSet.add_relationship() method later.
        """
        log.debug('Create RelationshipSet')
        if not batch_size:
            batch_size = self.batch_size
        log.debug('Batch Size: {}'.format(batch_size))

        i = 1
        # iterate over chunks of rels
        for batch in chunks(self.relationships, size=batch_size):

            log.debug('Batch {}'.format(i))

            create_relationships(graph.auto(),
                                 batch,
                                 self.rel_type,
                                 start_node_key=(tuple(self.start_node_labels), *self.fixed_order_start_node_properties),
                                 end_node_key=(tuple(self.end_node_labels), *self.fixed_order_end_node_properties))

            i += 1

    def merge(self, graph, batch_size=None):
        """
        Create relationships in this RelationshipSet
        """
        log.debug('Create RelationshipSet')
        if not batch_size:
            batch_size = self.batch_size
        log.debug('Batch Size: {}'.format(batch_size))

        fixed_order_start_node_properties = tuple(self.start_node_properties)
        fixed_order_end_node_properties = tuple(self.end_node_properties)
        print(fixed_order_start_node_properties, fixed_order_end_node_properties)

        i = 1
        # iterate over chunks of rels
        for batch in chunks(self.relationships, size=batch_size):
            log.debug('Batch {}'.format(i))

            merge_relationships(graph.auto(),
                                batch,
                                self.rel_type,
                                start_node_key=(tuple(self.start_node_labels), *self.fixed_order_start_node_properties),
                                end_node_key=(tuple(self.end_node_labels), *self.fixed_order_end_node_properties)
                                )

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
                create_composite_index(graph, label, self.start_node_properties)

        for label in self.end_node_labels:
            for prop in self.end_node_properties:
                create_single_index(graph, label, prop)

            # composite indexes
            if len(self.end_node_properties) > 1:
                create_composite_index(graph, label, self.end_node_properties)
