import logging
from uuid import uuid4
import os
import json
import csv
import gzip
from collections import defaultdict
from typing import Set, List, Union
from dataclasses import dataclass

from graphio.helper import chunks, create_single_index, create_composite_index, run_query_return_results
from graphio import defaults
from graphio.queries import nodes_merge_factory, nodes_create_factory
from graphio.defaults import BATCHSIZE as DEFAULT_BATCH_SIZE

log = logging.getLogger(__name__)

# dict with python types to casting functions in Cypher
CYPHER_TYPE_TO_FUNCTION = {int: 'toInteger',
                           float: 'toFloat'}

TYPE_CONVERSION = {'int': int,
                   'float': float}


@dataclass
class NodeModel:
    """
    Entrypoint for the application.
    """
    labels: List[str]
    merge_keys: List[str]
    default_props: dict = None
    preserve: List[str] = None
    append_props: List[str] = None
    additional_labels: List[str] = None



    def nodeset(self):
        """
        Create a NodeSet from this Node.

        :return: NodeSet
        """
        return NodeSet(labels=self.labels, merge_keys=self.merge_keys, default_props=self.default_props,
                       preserve=self.preserve, append_props=self.append_props, additional_labels=self.additional_labels)

    def dataset(self):
        return self.nodeset()


class NodeSet:
    """
    Container for a set of Nodes with the same labels and the same properties that define uniqueness.
    """

    def __init__(self, labels: List[str] = None, merge_keys: List[str] = None, default_props: dict = None,
                 preserve: List[str] = None, append_props: List[str] = None,
                 additional_labels: List[str] = None, indexed: bool = False):
        """


        """
        self.labels = labels
        self.merge_keys = merge_keys
        self.default_props = default_props or {}
        self.preserve = preserve or []
        self.append_props = append_props or []
        self.indexed = indexed
        self.additional_labels = additional_labels or []

        if self.labels:
            self.combined = '_'.join(sorted(self.labels)) + '_' + '_'.join(sorted(self.merge_keys))
        else:
            self.combined = '_'.join(sorted(self.merge_keys))
        self.uuid = str(uuid4())

        self.nodes = []
        # a node index with merge_key_id -> [positions in nodes list]
        # this works for both unique and non-unique settings
        self.node_index = defaultdict(list)

    def __str__(self):
        return f"<NodeSet ({self.labels}; {self.merge_keys})>"

    def _merge_key_id(self, node_dict: dict) -> tuple:
        """
        Create a FrozenSet from an ordered list of the merge_key properties for a node.

        :param node_dict: A node dict.
        :return:
        """
        return tuple([node_dict[key] for key in self.merge_keys])

    def add_node(self, properties):
        """
        Create a node in this NodeSet.

        :param properties: Node properties.
        :type properties: dict
        """
        if self.default_props:
            node_props = {**self.default_props, **properties}
        else:
            node_props = properties

        self.nodes.append(node_props)

        if self.indexed:
            self.node_index[self._merge_key_id(properties)].append(len(self.nodes) - 1)

    def add_nodes(self, list_of_properties):
        for properties in list_of_properties:
            self.add_node(properties)

    def update_node(self, properties: dict):
        """
        Update an existing node by overwriting all properties.

        Note that this requires `NodeSet(..., indexed=True)` which is not the default!

        :param properties: Node property dictionary.
        """
        if not self.indexed:
            raise TypeError("Update only works on an indexed NodeSet.")

        node_merge_key_id = self._merge_key_id(properties)
        if node_merge_key_id in self.node_index:
            # this function should work for single and multiple nodes
            for node_list_index in self.node_index[node_merge_key_id]:
                self.nodes[node_list_index].update(properties)
        # if the node does not exist it is created
        else:
            self.add_node(properties)

    def add_unique(self, properties):
        """
        Add a node to this NodeSet only if a node with the same `merge_keys` does not exist yet.

        Note: Right now this function iterates all nodes in the NodeSet. This is of course slow for large
        numbers of nodes. A better solution would be to create an 'index' as is done for RelationshipSet.

        :param properties: Node properties.
        :type properties: dict
        """

        compare_values = frozenset([properties[key] for key in self.merge_keys])

        for other_node_properties in self.node_properties():
            this_values = frozenset([other_node_properties[key] for key in self.merge_keys])
            if this_values == compare_values:
                return None

        # add node if not found
        self.add_node(properties)

    @property
    def metadata_dict(self):
        return {"labels": self.labels, "merge_keys": self.merge_keys}

    def to_dict(self):
        """
        Create dictionary defining the nodeset.
        """
        return {"labels": self.labels, "merge_keys": self.merge_keys, "nodes": self.nodes}

    @classmethod
    def from_dict(cls, nodeset_dict, batch_size=None):
        ns = cls(labels=nodeset_dict["labels"], merge_keys=nodeset_dict["merge_keys"])
        ns.add_nodes(nodeset_dict["nodes"])
        return ns

    def to_csv(self, filepath: str, quoting: int = None) -> str:
        """
        Create a CSV file for this nodeset. Header row is created with all properties.
        Each row contains the properties of a node.

        Example:

        >>> nodeset = NodeSet(labels=["Person"], merge_keys=["name"])
        >>> nodeset.add_node({"name": "Alice", "age": 33})
        >>> nodeset.add_node({"name": "Bob", "age": 44})
        >>> nodeset.to_csv("/tmp/Person_name.csv")
        '/tmp/Person_name.csv'

        name,age
        Alice,33
        Bob,44

        :param filepath: Full path to the CSV file.
        :param quoting: Optional quoting setting for csv writer (any of csv.QUOTE_MINIMAL, csv.QUOTE_NONE, csv.QUOTE_ALL etc).
        """

        if not quoting:
            quoting = csv.QUOTE_MINIMAL

        log.debug(f"Create CSV file {filepath} for NodeSet {self.combined}")

        all_props = self.all_property_keys()

        with open(filepath, 'w', newline='') as csvfile:
            fieldnames = list(all_props)
            writer = csv.DictWriter(csvfile, fieldnames=fieldnames, quoting=quoting)

            writer.writeheader()

            for n in self.nodes:
                writer.writerow(dict(n))

        return filepath

    def create_csv_query(self, filename: str = None, periodic_commit=1000):
        """
        Create a Cypher query to load a CSV file created with NodeSet.to_csv() into Neo4j (CREATE statement).

        :param filename: Optional filename. A filename will be autocreated if not passed.
        :param periodic_commit: Number of rows to commit in one transaction.
        :return: Cypher query.
        """

        property_types = self._estimate_type_of_property_values()

        if not filename:
            filename = f"{self.object_file_name()}.csv"

        q = "USING PERIODIC COMMIT {}\n".format(periodic_commit)
        q += "LOAD CSV WITH HEADERS FROM 'file:///{}' AS line\n".format(filename)
        q += "CREATE (n:{})\n".format(':'.join(self.labels))

        props_list = []
        for k in sorted(self.all_property_keys()):
            prop_type = property_types[k]
            if prop_type in CYPHER_TYPE_TO_FUNCTION:
                props_list.append(f"n.{k} = {CYPHER_TYPE_TO_FUNCTION[prop_type]}(line.{k})")
            else:
                props_list.append(f"n.{k} = line.{k}")

        q += "SET {}".format(', '.join(props_list))

        return q

    def merge_csv_query(self, filename: str = None, periodic_commit=1000):
        """
        Create a Cypher query to load a CSV file created with NodeSet.to_csv() into Neo4j (MERGE statement).

        :param filename: Optional filename. A filename will be autocreated if not passed.
        :param periodic_commit: Number of rows to commit in one transaction.
        :return: Cypher query.
        """

        property_types = self._estimate_type_of_property_values()

        if not filename:
            filename = f"{self.object_file_name()}.csv"

        merge_csv_query_elements = []
        for merge_key in self.merge_keys:
            prop_type = property_types[merge_key]
            if prop_type in CYPHER_TYPE_TO_FUNCTION:
                merge_csv_query_elements.append(f"{merge_key}: {CYPHER_TYPE_TO_FUNCTION[prop_type]}(line.{merge_key})")
            else:
                merge_csv_query_elements.append(f"{merge_key}: line.{merge_key}")
        merge_csv_query_string = ','.join(merge_csv_query_elements)

        q = "USING PERIODIC COMMIT {}\n".format(periodic_commit)
        q += "LOAD CSV WITH HEADERS FROM 'file:///{}' AS line\n".format(filename)
        q += f"MERGE (n:{':'.join(self.labels)} {{ {merge_csv_query_string} }})\n"

        props_list = []
        for k in sorted(self.all_property_keys()):
            prop_type = property_types[k]
            if prop_type in CYPHER_TYPE_TO_FUNCTION:
                props_list.append(f"n.{k} = {CYPHER_TYPE_TO_FUNCTION[prop_type]}(line.{k})")
            else:
                props_list.append(f"n.{k} = line.{k}")

        q += "SET {}".format(', '.join(props_list))

        return q

    def to_csv_json_set(self, csv_file_path, json_file_path, type_conversion: dict = None):
        """
        Write the default CSV/JSON file combination.

        Needs paths to CSV and JSON file.

        :param csv_file_path: Path to the CSV file.
        :param json_file_path: Path to the JSON file.
        :param type_conversion: Optional dictionary to convert types of properties.
        """
        self.to_csv(csv_file_path)
        with open(json_file_path, 'w') as f:
            json_dict = self.metadata_dict
            if type_conversion:
                json_dict['type_conversion'] = type_conversion
            json.dump(json_dict, f)

    @classmethod
    def from_csv_json_set(cls, csv_file_path, json_file_path, load_items: bool = False,
                          labels_key: str = None, mergekey_key: str = None):
        """
        Read the default CSV/JSON file combination. Needs paths to CSV and JSON file.

        JSON keys can be overwritten by passing the respective parameters.

        :param csv_file_path: Path to the CSV file.
        :param json_file_path: Path to the JSON file.
        :param load_items: Yield items from file (False, default) or load them to memory (True).
        :return: The NodeSet.
        """
        if not labels_key:
            labels_key = 'labels'
        if not mergekey_key:
            mergekey_key = 'merge_keys'

        with open(json_file_path) as f:
            metadata = json.load(f)

        # map properties
        property_map = None
        if 'property_map' in metadata:
            # replace mergekeys if necessary
            property_map = metadata['property_map']
            metadata[mergekey_key] = [property_map[x] if x in property_map else x for x in metadata[mergekey_key]]

        # type conversion
        type_conversion = metadata.get('type_conversion', None)

        # NodeSet instance
        nodeset = cls(metadata[labels_key], merge_keys=metadata[mergekey_key])

        if load_items:
            nodeset.nodes = _read_nodes(csv_file_path, property_map, type_conversion)
        else:
            nodeset.nodes = _yield_node(csv_file_path, property_map, type_conversion)

        return nodeset

    def object_file_name(self, suffix: str = None) -> str:
        """
        Create a unique name for this NodeSet that indicates content. Pass an optional suffix.
        NOTE: suffix has to include the '.' for a filename!

            `nodeset_Label_merge-key_uuid`

        With suffix:

            `nodeset_Label_merge-key_uuid.json`
        """
        basename = f"nodeset_{'_'.join(self.labels)}_{'_'.join(self.merge_keys)}_{self.uuid}"
        if suffix:
            basename += suffix
        return basename

    def to_json(self, target_dir: str, filename: str = None):
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

    def create(self, graph, database: str = None, batch_size=None):
        """
        Create all nodes from NodeSet.
        """
        log.debug('Create NodeSet')
        if not batch_size:
            batch_size = DEFAULT_BATCH_SIZE
        log.debug('Batch Size: {}'.format(batch_size))

        q = nodes_create_factory(self.labels, property_parameter="props", additional_labels=self.additional_labels)

        for batch in chunks(self.nodes, size=batch_size):
            run_query_return_results(graph, q, database=database, props=list(batch), source=self.uuid)

    def merge(self, graph, merge_properties=None, batch_size=None, preserve=None, append_props=None, database=None):
        """
        Merge nodes from NodeSet on merge properties.

        :param merge_properties: The merge properties.
        """
        if not self.labels:
            log.warning("MERGing without labels will not use an index and is slow.")
        # overwrite if preserve is passed
        if preserve:
            self.preserve = preserve
        # overwrite if array_props is passed
        if append_props:
            self.append_props = append_props

        log.debug('Merge NodeSet on {}'.format(merge_properties))

        if not batch_size:
            batch_size = DEFAULT_BATCH_SIZE

        if not merge_properties:
            merge_properties = self.merge_keys

        log.debug('Batch Size: {}'.format(batch_size))

        q = nodes_merge_factory(self.labels, self.merge_keys, array_props=self.append_props, preserve=self.preserve,
                                property_parameter='props', additional_labels=self.additional_labels)

        for batch in chunks(self.node_properties(), size=batch_size):
            run_query_return_results(graph, q, database=database, props=list(batch), append_props=self.append_props,
                                     preserve=self.preserve, source=self.uuid)

    def node_properties(self):
        """
        Yield properties of the nodes in this set. Used for create function.
        """
        for n in self.nodes:
            yield dict(n)

    def all_property_keys(self) -> Set[str]:
        """
        Return a set of all property keys in this NodeSet

        :return: A set of unique property keys of a NodeSet
        """
        all_props = set()

        # collect properties
        for props in self.node_properties():
            for k in props:
                all_props.add(k)

        return all_props

    def _estimate_type_of_property_values(self):
        """
        To create data from CSV we need to know the type of all node properties.

        This function tries to find the type and falls back to string if it's not consistent. For performance reasons
        this function is limited to the first 1000 nodes.

        :return:
        """
        property_types = {}
        for p in self.all_property_keys():
            this_type = None
            for node in self.nodes[:1000]:
                try:
                    value = node[p]
                    type_of_value = type(value)
                except KeyError:
                    type_of_value = None

                if not this_type:
                    this_type = type_of_value
                else:
                    if this_type != type_of_value:
                        this_type = str
                        break

            property_types[p] = this_type

        return property_types

    def create_index(self, graph, database=None):
        """
        Create indices for all label/merge ky combinations as well as a composite index if multiple merge keys exist.
        """
        if self.merge_keys:
            for label in self.labels:
                # create individual indexes
                for prop in self.merge_keys:
                    create_single_index(graph, label, prop, database)

                # composite indexes
                if len(self.merge_keys) > 1:
                    create_composite_index(graph, label, self.merge_keys, database)


def _read_nodes(csv_filepath, property_map, type_conversion=None):
    """
    Instead of recreating the entire RelationShip set in memory this function yields
    one relationship at a time.

    :param csv_filepath: Path to the CSV file.
    :param property_map: Property map to rename properties.
    :return: One node property dict per iteration.
    """

    if csv_filepath.endswith('.gz'):
        csvfile = gzip.open(csv_filepath, 'rt')
    else:
        csvfile = open(csv_filepath, newline='')
    lines = csvfile.readlines()
    csvfile.close()

    # get header line
    header = lines[0].strip().split(',')
    header = [x.replace('"', '') for x in header]

    log.debug(f"Header: {header}")

    if property_map:
        log.debug(f"Replace header {header}")
        header = [property_map[x] if x in property_map else x for x in header]
        log.debug(f"With header {header}")

    nodes = []
    rdr = csv.DictReader(lines[1:], fieldnames=header)
    for node in rdr:
        if type_conversion:
            for node_key, node_value in node.items():
                if node_key in type_conversion:
                    node[node_key] = TYPE_CONVERSION[type_conversion[node_key]](node_value)
        nodes.append(node)

    return nodes


def _yield_node(csv_filepath, property_map, type_conversion=None):
    """
    Instead of recreating the entire RelationShip set in memory this function yields
    one relationship at a time.

    :param csv_filepath: Path to the CSV file.
    :param property_map: Property map to rename properties.
    :return: One node property dict per iteration.
    """

    if csv_filepath.endswith('.gz'):
        csvfile = gzip.open(csv_filepath, 'rt')
    else:
        csvfile = open(csv_filepath, newline='')

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
    for node in rdr:
        if type_conversion:
            for k, v in node.items():
                if k in type_conversion:
                    node[k] = TYPE_CONVERSION[type_conversion[k]](v)
        yield node
    csvfile.close()
