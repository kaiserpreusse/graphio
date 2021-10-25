from uuid import uuid4
import logging
import json
import os
import csv
from py2neo.bulk import create_relationships, merge_relationships
from typing import Set

from graphio import defaults
from graphio.helper import chunks, create_single_index, create_composite_index

log = logging.getLogger(__name__)

# dict with python types to casting functions in Cypher
CYPHER_TYPE_TO_FUNCTION = {int: 'toInteger',
                           float: 'toFloat'}


def tuplify_json_list(list_object: list) -> tuple:
    """
    JSON.dump() stores tuples as JSON lists. This function receives a list (with sub lists)
    and creates a tuple of tuples from the list. The tuples are the preferred input type for py2neo.

    E.g.

        [[a, b], [c, d]] -> ((a, b), (c, d))

    :param list_object: A list with sub-lists.
    :return: A tuple version of the list object.
    """
    output = tuple()
    for element in list_object:
        if isinstance(element, list):
            output = output + (tuple(element), )
        else:
            output = output + (element, )
    return output


class RelationshipSet:
    """
    Container for a set of Relationships with the same type of start and end nodes.
    """

    def __init__(self, rel_type, start_node_labels, end_node_labels, start_node_properties, end_node_properties,
                 batch_size=None, default_props=None):
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
        self.default_props = default_props

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

    def __str__(self):
        return f"<RelationshipSet ({self.start_node_labels}; {self.start_node_properties})-[{self.rel_type}]->({self.end_node_labels}; {self.end_node_properties})>"

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
                self.relationships.append(self.__relationship_from_dictionary(start_node_properties, end_node_properties, rel_props))
                self.unique_rels.add(check_set)
        else:
            self.relationships.append(self.__relationship_from_dictionary(start_node_properties, end_node_properties, rel_props))

    def all_property_keys(self) -> Set[str]:
        """
        Return a set of all property keys in this RelationshipSet

        :return: A set of unique property keys of a NodeSet
        """
        all_props = set()

        # collect properties
        for r in self.relationships:
            all_props.update(r[1].keys())

        return all_props

    def _estimate_type_of_property_values(self):
        """
        To create data from CSV we need to know the type of start/end node properties as well as relationship properties.

        This function tries to find the type and falls back to string if it's not consistent. For performance reasons
        this function is limited to the first 100 relationships.

        :return:
        """
        start_node_property_types = {}
        for i, p in enumerate(self.fixed_order_start_node_properties):
            this_type = None
            for rel in self.relationships[:100]:
                if isinstance(rel[0], tuple):
                    value = rel[0][i]
                else:
                    value = rel[0]
                type_of_value = type(value)
                if not this_type:
                    this_type = type_of_value
                else:
                    if this_type != type_of_value:
                        this_type = str
                        break
            start_node_property_types[p] = this_type

        end_node_property_types = {}
        for i, p in enumerate(self.fixed_order_end_node_properties):
            this_type = None
            for rel in self.relationships[:100]:

                if isinstance(rel[2], tuple):
                    value = rel[2][i]
                else:
                    value = rel[2]

                type_of_value = type(value)
                if not this_type:
                    this_type = type_of_value
                else:
                    if this_type != type_of_value:
                        this_type = str
                        break
            end_node_property_types[p] = this_type

        rel_property_types = {}

        for p in self.all_property_keys():
            this_type = None
            for rel in self.relationships[:100]:
                try:
                    value = rel[1][p]
                    type_of_value = type(value)
                except KeyError:
                    type_of_value = None

                if not this_type:
                    this_type = type_of_value
                else:
                    if this_type != type_of_value:
                        this_type = str
                        break

            rel_property_types[p] = this_type

        return start_node_property_types, rel_property_types, end_node_property_types

    def to_dict(self):
        return {"rel_type": self.rel_type,
                "start_node_labels": self.start_node_labels,
                "end_node_labels": self.end_node_labels,
                "start_node_properties": self.start_node_properties,
                "end_node_properties": self.end_node_properties,
                "unique": self.unique,
                "relationships": self.relationships}

    @classmethod
    def from_csv_with_header(cls, path):

        header = {}
        # get header
        with open(path, 'rt') as f:
            for l in f:
                if l.startswith('#'):
                    l = l.replace('#', '').strip()
                    k, v = l.split(',')
                    if '|' in v:
                        v = v.split('|')
                    else:
                        v = [v]
                    header[k] = v
                else:
                    break
        print(header)

        header['type'] = header['type'][0]

        rs = cls(header['type'], header['start_node_labels'], header['end_node_labels'], header['start_node_keys'], header['end_node_keys'])

        start_key_to_header = {}
        for k in rs.start_node_properties:
            start_key_to_header[k] = f"start_{k}"

        end_key_to_header = {}
        for k in rs.end_node_properties:
            end_key_to_header[k] = f"end_{k}"

        with open(path, newline='') as csvfile:

            rdr = csv.DictReader(row for row in csvfile if not row.startswith('#'))

            for row in rdr:
                start_dict = {}
                for k in rs.start_node_properties:
                    start_dict[k] = row[start_key_to_header[k]]
                end_dict = {}
                for k in rs.end_node_properties:
                    end_dict[k] = row[end_key_to_header[k]]
                # get properties
                properties = {}
                for k, v in row.items():
                    if k.startswith('rel_'):
                        k = k.replace('rel_', '')
                        properties[k] = v


                rs.add_relationship(start_dict, end_dict, properties)

        return rs

    def to_csv(self, filepath: str, filename: str = None, quoting: int = None) -> str:
        """
        Note: You can't use arrays as properties for nodes/relationships when creating CSV files.

        LOAD CSV WITH HEADERS FROM xyz AS line
        MATCH (a:Gene), (b:GeneSymbol)
        WHERE a.sid = line.a_sid AND b.sid = line.b_sid AND b.taxid = line.b_taxid
        CREATE (a)-[r:MAPS]->(b)
        SET r.key1 = line.rel_key1, r.key2 = line.rel_key2

        # CSV file header
        a_sid, b_sid, b_taxid, rel_key1, rel_key2

        :param filepath: Path to csv file.
        :param relset: The RelationshipSet
        :type relset: graphio.RelationshipSet
        """
        if not filename:
            filename = f"{self.object_file_name()}.csv"
        if not quoting:
            quoting = csv.QUOTE_MINIMAL

        csv_file_path = os.path.join(filepath, filename)

        header = []

        for prop in self.start_node_properties:
            header.append("a_{}".format(prop))

        for prop in self.end_node_properties:
            header.append("b_{}".format(prop))

        for prop in self.all_property_keys():
            header.append("rel_{}".format(prop))

        with open(csv_file_path, 'w', newline='') as csvfile:
            writer = csv.DictWriter(csvfile, fieldnames=header, quoting=quoting)

            writer.writeheader()

            for rel in self.relationships:
                # create data for row
                rel_csv_dict = {}
                for i, k in enumerate(self.fixed_order_start_node_properties):

                    if isinstance(rel[0], tuple):
                        rel_csv_dict["a_{}".format(k)] = rel[0][i]
                    else:
                        rel_csv_dict["a_{}".format(k)] = rel[0]

                for i, k in enumerate(self.fixed_order_end_node_properties):
                    if isinstance(rel[2], tuple):
                        rel_csv_dict["b_{}".format(k)] = rel[2][i]
                    else:
                        rel_csv_dict["b_{}".format(k)] = rel[2]

                for k, v in rel[1].items():
                    rel_csv_dict["rel_{}".format(k)] = v

                writer.writerow(rel_csv_dict)

        return csv_file_path

    def csv_query(self, query_type: str, filename: str = None, periodic_commit=1000) -> str:
        """
        Generate the CREATE CSV query for this RelationshipSet. The function tries to take care of type conversions.

        Note: You can't use arrays as properties for nodes/relationships when creating CSV files.

        LOAD CSV WITH HEADERS FROM xyz AS line
        MATCH (a:Gene), (b:Protein)
        WHERE a.sid = line.a_sid AND b.sid = line.b_sid AND b.taxid = line.b_taxid
        CREATE (a)-[r:MAPS]->(b)
        SET r.key1 = line.rel_key1, r.key2 = line.rel_key2
        """
        if query_type not in ['CREATE', 'MERGE']:
            raise ValueError(f"Can only use query_type 'CREATE' or 'MERGE', not {query_type}")

        if not filename:
            filename = f"{self.object_file_name()}.csv"

        # get types
        start_node_property_types, rel_property_types, end_node_property_types = self._estimate_type_of_property_values()

        q = "USING PERIODIC COMMIT {} \n".format(periodic_commit)
        q += "LOAD CSV WITH HEADERS FROM 'file:///{}' AS line \n".format(filename)
        q += "MATCH (a:{0}), (b:{1}) \n".format(':'.join(self.start_node_labels), ':'.join(self.end_node_labels))

        where_clauses = []
        for prop in self.fixed_order_start_node_properties:
            prop_type = start_node_property_types[prop]
            if prop_type in CYPHER_TYPE_TO_FUNCTION:
                where_clauses.append(f"a.{prop} = {CYPHER_TYPE_TO_FUNCTION[prop_type]}(line.a_{prop})")
            else:
                where_clauses.append("a.{0} = line.a_{0}".format(prop))

        for prop in self.fixed_order_end_node_properties:
            prop_type = end_node_property_types[prop]
            if prop_type in CYPHER_TYPE_TO_FUNCTION:
                where_clauses.append(f"b.{prop} = {CYPHER_TYPE_TO_FUNCTION[prop_type]}(line.b_{prop})")
            else:
                where_clauses.append("b.{0} = line.b_{0}".format(prop))

        q += "WHERE {} \n".format(" AND ".join(where_clauses))

        q += f"{query_type} (a)-[r:{self.rel_type}]->(b) \n"

        rel_prop_list = []
        for prop in sorted(self.all_property_keys()):
            prop_type = rel_property_types[prop]
            if prop_type in CYPHER_TYPE_TO_FUNCTION:
                rel_prop_list.append(f"r.{prop} = {CYPHER_TYPE_TO_FUNCTION[prop_type]}(line.rel_{prop})")
            else:
                rel_prop_list.append("r.{0} = line.rel_{0}".format(prop))

        q += "SET {}".format(", ".join(rel_prop_list))

        return q

    @classmethod
    def from_dict(cls, relationship_dict, batch_size=None):
        rs = cls(rel_type=relationship_dict["rel_type"],
                 start_node_labels=relationship_dict["start_node_labels"],
                 end_node_labels=relationship_dict["end_node_labels"],
                 start_node_properties=relationship_dict["start_node_properties"],
                 end_node_properties=relationship_dict["end_node_properties"],
                 batch_size=batch_size)
        rs.unique = relationship_dict["unique"]
        rs.relationships = [tuplify_json_list(r) for r in relationship_dict["relationships"]]

        return rs

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

    def serialize(self, target_dir: str):
        """
        Serialize NodeSet to a JSON file in a target directory.
        """
        path = os.path.join(target_dir, self.object_file_name(suffix='.json'))
        with open(path, 'wt') as f:
            json.dump(self.to_dict(), f, indent=4)

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

        # iterate over chunks of rels
        for batch in chunks(self.relationships, size=batch_size):

            create_relationships(graph.auto(),
                                 batch,
                                 self.rel_type,
                                 start_node_key=(tuple(self.start_node_labels), *self.fixed_order_start_node_properties),
                                 end_node_key=(tuple(self.end_node_labels), *self.fixed_order_end_node_properties))


    def merge(self, graph, batch_size=None):
        """
        Create relationships in this RelationshipSet
        """
        log.debug('Create RelationshipSet')
        if not batch_size:
            batch_size = self.batch_size
        log.debug('Batch Size: {}'.format(batch_size))

        # iterate over chunks of rels
        for batch in chunks(self.relationships, size=batch_size):

            merge_relationships(graph.auto(),
                                batch,
                                self.rel_type,
                                start_node_key=(tuple(self.start_node_labels), *self.fixed_order_start_node_properties),
                                end_node_key=(tuple(self.end_node_labels), *self.fixed_order_end_node_properties)
                                )


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
