from uuid import uuid4
import logging
import json
import os
import csv
from typing import Set, List
import gzip

from graphio import defaults
from graphio.helper import chunks, create_single_index, create_composite_index
from graphio.queries import rels_create_unwind, rels_merge_unwind, rels_params_from_objects
from graphio.graph import run_query_return_results

log = logging.getLogger(__name__)

# dict with python types to casting functions in Cypher
CYPHER_TYPE_TO_FUNCTION = {int: 'toInteger',
                           float: 'toFloat'}

TYPE_CONVERSION = {'int': int,
                   'float': float}


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
            output = output + (tuple(element),)
        else:
            output = output + (element,)
    return output


class RelationshipSet:
    """
    Container for a set of Relationships with the same type of start and end nodes.
    """

    def __init__(self, rel_type, start_node_labels, end_node_labels, start_node_properties, end_node_properties,
                 batch_size=None, default_props=None):
        """

        :param rel_type: Realtionship type.
        :param start_node_labels: Labels of the start node.
        :param end_node_labels: Labels of the end node.
        :param start_node_properties: Property keys to identify the start node.
        :param end_node_properties: Properties to identify the end node.
        :param batch_size: Batch size for Neo4j operations.
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

        if batch_size:
            self.batch_size = batch_size
        else:
            self.batch_size = defaults.BATCHSIZE

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

    def _estimate_type_of_property_values(self):
        """
        To create data from CSV we need to know the type of start/end node properties as well as relationship properties.

        This function tries to find the type and falls back to string if it's not consistent. For performance reasons
        this function is limited to the first 100 relationships.

        :return:
        """
        start_node_property_types = {}
        for p in self.start_node_properties:
            this_type = None
            for rel in self.relationships[:100]:
                value = rel[0][p]
                type_of_value = type(value)
                if not this_type:
                    this_type = type_of_value
                else:
                    if this_type != type_of_value:
                        this_type = str
                        break
            start_node_property_types[p] = this_type

        end_node_property_types = {}
        for p in self.end_node_properties:
            this_type = None
            for rel in self.relationships[:100]:

                value = rel[1][p]

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
                    value = rel[2][p]
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

    @property
    def metadata_dict(self):
        return {"rel_type": self.rel_type,
                "start_node_labels": self.start_node_labels,
                "end_node_labels": self.end_node_labels,
                "start_node_properties": self.start_node_properties,
                "end_node_properties": self.end_node_properties}

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
        rs.relationships = [tuplify_json_list(r) for r in relationship_dict["relationships"]]

        return rs

    def to_csv_json_set(self, csv_file_path, json_file_path, write_mode: str = 'w'):
        """
        Write the default CSV/JSON file combination.

        Needs paths to CSV and JSON file.

        :param csv_file_path: Path to the CSV file.
        :param json_file_path: Path to the JSON file.
        :param write_mode: Write mode for the CSV file.
        """
        self.to_csv(csv_file_path)
        with open(json_file_path, write_mode) as f:
            json.dump(self.metadata_dict, f)

    @classmethod
    def from_csv_json_set(cls, csv_file_path, json_file_path, load_items: bool = False):
        """
        Read the default CSV/JSON file combination.

        Needs paths to CSV and JSON file.

        :param csv_file_path: Path to the CSV file.
        :param json_file_path: Path to the JSON file.
        :param load_items: Yield items from file (False, default) or load them to memory (True).
        :return: The RelationshipSet.
        """
        with open(json_file_path) as f:
            metadata = json.load(f)

        # map properties
        property_map = None
        if 'property_map' in metadata:
            # replace start_node/end_node keys if necessary
            property_map = metadata['property_map']
            metadata['start_node_properties'] = [property_map[x] if x in property_map else x for x in
                                                 metadata['start_node_properties']]
            metadata['end_node_properties'] = [property_map[x] if x in property_map else x for x in
                                               metadata['end_node_properties']]

        # type conversions
        start_node_type_conversion = metadata.get('start_node_type_conversion', None)
        end_node_type_conversion = metadata.get('end_node_type_conversion', None)

        # RelationshipSet instance
        rs = cls(metadata['rel_type'],
                 metadata['start_node_labels'],
                 metadata['end_node_labels'],
                 remove_prefix_from_keys(metadata['start_node_properties']),
                 remove_prefix_from_keys(metadata['end_node_properties']))

        start_key_to_header = {}
        for k in rs.start_node_properties:
            start_key_to_header[k] = f"start_{k}"

        end_key_to_header = {}
        for k in rs.end_node_properties:
            end_key_to_header[k] = f"end_{k}"

        if load_items:
            rs.relationships = _read_rels(csv_file_path, rs.start_node_properties, rs.end_node_properties,
                                          start_key_to_header, end_key_to_header, property_map,
                                          start_node_type_conversion, end_node_type_conversion)
        else:
            rs.relationships = _yield_rels(csv_file_path, rs.start_node_properties, rs.end_node_properties,
                                           start_key_to_header, end_key_to_header, property_map,
                                           start_node_type_conversion, end_node_type_conversion)

        return rs

    def to_csv(self, filepath: str, quoting: int = None) -> str:
        """
        Write the RelationshipSet to a CSV file. The CSV file will be written to the given filepath.

        Note: You can't use arrays as properties for nodes/relationships when creating CSV files.

        # CSV file header
        start_sid, end_sid, end_taxid, rel_key1, rel_key2

        :param filepath: Path to csv file.
        :param relset: The RelationshipSet
        :type relset: graphio.RelationshipSet
        """
        if not quoting:
            quoting = csv.QUOTE_MINIMAL

        header = []

        for prop in self.start_node_properties:
            header.append("start_{}".format(prop))

        for prop in self.end_node_properties:
            header.append("end_{}".format(prop))

        for prop in self.all_property_keys():
            header.append("rel_{}".format(prop))

        with open(filepath, 'w', newline='') as csvfile:
            writer = csv.DictWriter(csvfile, fieldnames=header, quoting=quoting)

            writer.writeheader()

            for rel in self.relationships:
                # create data for row
                rel_csv_dict = {}
                for k in self.start_node_properties:
                    rel_csv_dict["start_{}".format(k)] = rel[0][k]

                for k in self.end_node_properties:
                    rel_csv_dict["end_{}".format(k)] = rel[1][k]

                for k, v in rel[2].items():
                    rel_csv_dict["rel_{}".format(k)] = v

                writer.writerow(rel_csv_dict)

        return filepath

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

    def to_json(self, target_dir, filename: str = None):
        """
        Serialize NodeSet to a JSON file in a target directory.

        This function is meant for dumping/reloading and not to create a general transport
        format. The function will likely be optimized for disk space or compressed in future.
        """
        if not filename:
            filename = self.object_file_name(suffix='.json')
        path = os.path.join(target_dir, filename)
        with open(path, 'wt') as f:
            json.dump(self.to_dict(), f, indent=4)

    def create(self, graph, database=None, batch_size=None):
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
        q = rels_create_unwind(self.start_node_labels, self.end_node_labels, self.start_node_properties,
                               self.end_node_properties, self.rel_type)
        for batch in chunks(self.relationships, size=batch_size):
            query_parameters = rels_params_from_objects(batch)
            run_query_return_results(graph, q, database=database, **query_parameters)

    def merge(self, graph, database=None, batch_size=None):
        """
        Create relationships in this RelationshipSet
        """
        log.debug('Create RelationshipSet')
        if not batch_size:
            batch_size = self.batch_size
        log.debug('Batch Size: {}'.format(batch_size))

        # iterate over chunks of rels
        q = rels_merge_unwind(self.start_node_labels, self.end_node_labels, self.start_node_properties,
                              self.end_node_properties, self.rel_type)
        for batch in chunks(self.relationships, size=batch_size):
            query_parameters = rels_params_from_objects(batch)
            run_query_return_results(graph, q, database=database, **query_parameters)

    def create_index(self, graph, database=None):
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


def _read_rels(csv_filepath, start_node_properties, end_node_properties, start_key_to_header, end_key_to_header,
               property_map, start_node_type_conversion: dict, end_node_type_conversion: dict):
    if csv_filepath.endswith('.gz'):
        csvfile = gzip.open(csv_filepath, 'rt')
    else:
        csvfile = open(csv_filepath, newline='')

    lines = csvfile.readlines()
    csvfile.close()

    header = lines[0].strip().split(',')
    header = [x.replace('"', '') for x in header]

    log.debug(f"Header: {header}")

    if property_map:
        log.debug(f"Replace header {header}")
        header = [property_map[x] if x in property_map else x for x in header]
        log.debug(f"With header {header}")

    rdr = csv.DictReader(lines[1:], fieldnames=header)

    relationships = []

    for row in rdr:

        start_node_data = dict(zip(start_node_properties, [row[start_key_to_header[x]] for x in start_node_properties]))
        if start_node_type_conversion:
            for k, v in start_node_data.items():
                if k in start_node_type_conversion:
                    start_node_data[k] = TYPE_CONVERSION[start_node_type_conversion[k]](v)

        end_node_data = dict(zip(end_node_properties, [row[end_key_to_header[x]] for x in end_node_properties]))
        if end_node_type_conversion:
            for k, v in end_node_data.items():
                if k in end_node_type_conversion:
                    end_node_data[k] = TYPE_CONVERSION[end_node_type_conversion[k]](v)

        # get properties
        properties = {}
        for k, v in row.items():
            if k.startswith('rel_'):
                k = k.replace('rel_', '')
                properties[k] = v
        relationships.append((start_node_data, end_node_data, properties))

    return relationships


def _yield_rels(csv_filepath, start_node_properties, end_node_properties, start_key_to_header, end_key_to_header,
                property_map, start_node_type_conversion, end_node_type_conversion):
    """
    Instead of recreating the entire RelationShip set in memory this function yields
    one relationship at a time.

    Note that there is some data conversion going on to return relationships in the
    format introduced when graphio base functions were merged into py2neo.

    :param csv_filepath:
    :param start_node_properties:
    :param end_node_properties:
    :param start_key_to_header:
    :param end_key_to_header:
    :param property_map:
    :return:
    """

    if csv_filepath.endswith('.gz'):
        csvfile = gzip.open(csv_filepath, 'rt')
    else:
        csvfile = open(csv_filepath, newline='')

    # get header line
    # get header line
    header = None
    while not header:
        line = csvfile.readline()
        if not line.startswith('#'):
            header = line.strip().split(',')
            header = [x.replace('"', '') for x in header]
    log.debug(f"Header: {header}")

    if property_map:
        log.debug(f"Replace header {header}")
        header = [property_map[x] if x in property_map else x for x in header]
        log.debug(f"With header {header}")

    rdr = csv.DictReader([row for row in csvfile if not row.startswith('#')], fieldnames=header)

    for row in rdr:

        start_node_data = dict(zip(start_node_properties, [row[start_key_to_header[x]] for x in start_node_properties]))
        if start_node_type_conversion:
            for k, v in start_node_data.items():
                if k in start_node_type_conversion:
                    start_node_data[k] = TYPE_CONVERSION[start_node_type_conversion[k]](v)

        end_node_data = dict(zip(end_node_properties, [row[end_key_to_header[x]] for x in end_node_properties]))
        if end_node_type_conversion:
            for k, v in end_node_data.items():
                if k in end_node_type_conversion:
                    end_node_data[k] = TYPE_CONVERSION[end_node_type_conversion[k]](v)

        # get properties
        properties = {}
        for k, v in row.items():
            if k.startswith('rel_'):
                k = k.replace('rel_', '')
                properties[k] = v
        yield (start_node_data, end_node_data, properties)

    csvfile.close()


def remove_prefix_from_keys(keys: list) -> List[str]:
    """
    In some JSON files the start/end node keys contain `start_` or `end_`.

    This is only required in the CSV file to properly distinguish columns, not in
    the RelationshipSet or JSON file where we have a dedicated place to store start/end node keys.

    This function simply removes the prefix but does not fail if they do not exist.

    :param keys: The list of keys.
    :return: Cleaned list of keys without prefix.
    """
    output = []
    for k in keys:
        if k.startswith('start_'):
            output.append(k.split('_', 1)[1])
        elif k.startswith('end_'):
            output.append(k.split('_', 1)[1])
        else:
            output.append(k)
    return output
