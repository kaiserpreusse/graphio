import logging
from uuid import uuid4
from py2neo.bulk import create_nodes, merge_nodes
import os
import json
import csv

from graphio.helper import chunks, create_single_index, create_composite_index
from graphio import defaults
from graphio.objects.relationshipset import RelationshipSet
from graphio.queries import nodes_merge_unwind_preserve, nodes_merge_unwind_array_props, \
    nodes_merge_unwind_preserve_array_props

log = logging.getLogger(__name__)

# dict with python types to casting functions in Cypher
CYPHER_TYPE_TO_FUNCTION = {int: 'toInteger',
                           float: 'toFloat'}


class NodeSet:
    """
    Container for a set of Nodes with the same labels and the same properties that define uniqueness.
    """

    def __init__(self, labels, merge_keys=None, batch_size=None, default_props=None, preserve=None, append_props=None):
        """

        :param labels: The labels for the nodes in this NodeSet.
        :type labels: list[str]
        :param merge_keys: The properties that define uniqueness of the nodes in this NodeSet.
        :type merge_keys: list[str]
        :param batch_size: Batch size for Neo4j operations.
        :type batch_size: int
        """
        self.labels = labels
        self.merge_keys = merge_keys
        self.default_props = default_props
        self.preserve = preserve
        self.append_props = append_props

        self.combined = '_'.join(sorted(self.labels)) + '_' + '_'.join(sorted(self.merge_keys))
        self.uuid = str(uuid4())

        if batch_size:
            self.batch_size = batch_size
        else:
            self.batch_size = defaults.BATCHSIZE

        self.nodes = []

    def __str__(self):
        return f"<NodeSet ({self.labels}; {self.merge_keys})>"

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

    def add_nodes(self, list_of_properties):
        for properties in list_of_properties:
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

    def to_dict(self):
        """
        Create dictionary defining the nodeset.
        """
        return {"labels": self.labels, "merge_keys": self.merge_keys, "nodes": self.nodes}

    def to_csv(self, filepath: str, filename: str = None, quoting: int = None) -> str:
        """
        Create a CSV file for this nodeset.

        :param filepath: Path where the file is stored.
        :param filename: Optional filename. A filename will be autocreated if not passed.
        :param quoting: Optional quoting setting for csv writer (any of csv.QUOTE_MINIMAL, csv.QUOTE_NONE, csv.QUOTE_ALL etc).
        """
        if not filename:
            filename = f"{self.object_file_name()}.csv"
        if not quoting:
            quoting = csv.QUOTE_MINIMAL

        csv_file_path = os.path.join(filepath, filename)

        log.debug(f"Create CSV file {csv_file_path}")

        all_props = self.all_properties_in_nodeset()

        with open(csv_file_path, 'w', newline='') as csvfile:
            fieldnames = list(all_props)
            writer = csv.DictWriter(csvfile, fieldnames=fieldnames, quoting=quoting)

            writer.writeheader()

            for n in self.nodes:
                writer.writerow(dict(n))

        return csv_file_path

    def create_csv_query(self, filename: str = None, periodic_commit=1000):

        # get types
        property_types = self._estimate_type_of_property_values()

        if not filename:
            filename = f"{self.object_file_name()}.csv"

        q = "USING PERIODIC COMMIT {}\n".format(periodic_commit)
        q += "LOAD CSV WITH HEADERS FROM 'file:///{}' AS line\n".format(filename)
        q += "CREATE (n:{})\n".format(':'.join(self.labels))

        props_list = []
        for k in sorted(self.all_properties_in_nodeset()):
            prop_type = property_types[k]
            if prop_type in CYPHER_TYPE_TO_FUNCTION:
                props_list.append(f"n.{k} = {CYPHER_TYPE_TO_FUNCTION[prop_type]}(line.{k})")
            else:
                props_list.append(f"n.{k} = line.{k}")

        q += "SET {}".format(', '.join(props_list))

        return q

    def merge_csv_query(self, filename: str = None, periodic_commit=1000):
        # get types
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
        for k in sorted(self.all_properties_in_nodeset()):
            prop_type = property_types[k]
            if prop_type in CYPHER_TYPE_TO_FUNCTION:
                props_list.append(f"n.{k} = {CYPHER_TYPE_TO_FUNCTION[prop_type]}(line.{k})")
            else:
                props_list.append(f"n.{k} = line.{k}")

        q += "SET {}".format(', '.join(props_list))

        return q

    @classmethod
    def from_dict(cls, nodeset_dict, batch_size=None):
        ns = cls(labels=nodeset_dict["labels"], merge_keys=nodeset_dict["merge_keys"])
        ns.add_nodes(nodeset_dict["nodes"])
        return ns

    @classmethod
    def from_csv_with_header(cls, path):

        header = {}
        # get header
        log.debug(f"Read file {path} into NodeSet.")

        with open(path, 'rt') as f:
            log.debug(f)
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

        nodeset = cls(labels=header['labels'], merge_keys=header['merge_keys'])

        with open(path, newline='') as csvfile:

            rdr = csv.DictReader(row for row in csvfile if not row.startswith('#'))
            for node in rdr:
                nodeset.add_node(node)

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

    def serialize(self, target_dir: str):
        """
        Serialize NodeSet to a JSON file in a target directory.
        """
        path = os.path.join(target_dir, self.object_file_name(suffix='.json'))
        with open(path, 'wt') as f:
            json.dump(self.to_dict(), f, indent=4)

    def create(self, graph, batch_size=None):
        """
        Create all nodes from NodeSet.
        """
        log.debug('Create NodeSet')
        if not batch_size:
            batch_size = self.batch_size
        log.debug('Batch Size: {}'.format(batch_size))

        for batch in chunks(self.nodes, size=batch_size):
            create_nodes(graph, batch, labels=self.labels)

    def merge(self, graph, merge_properties=None, batch_size=None, preserve=None, append_props=None):
        """
        Merge nodes from NodeSet on merge properties.

        :param merge_properties: The merge properties.
        """
        # overwrite if preserve is passed
        if preserve:
            self.preserve = preserve
        # overwrite if array_props is passed
        if append_props:
            self.append_props = append_props

        log.debug('Merge NodeSet on {}'.format(merge_properties))

        if not batch_size:
            batch_size = self.batch_size

        if not merge_properties:
            merge_properties = self.merge_keys

        log.debug('Batch Size: {}'.format(batch_size))

        # use py2neo base functions if no properties are preserved
        if not self.preserve and not self.append_props:
            for batch in chunks(self.node_properties(), size=batch_size):
                merge_nodes(graph, batch, (tuple(self.labels), *merge_properties))

        elif self.preserve and not self.append_props:
            q = nodes_merge_unwind_preserve(self.labels, self.merge_keys, property_parameter='props')
            for batch in chunks(self.node_properties(), size=batch_size):
                graph.run(q, props=list(batch), preserve=self.preserve)

        elif not self.preserve and self.append_props:
            q = nodes_merge_unwind_array_props(self.labels, self.merge_keys, self.append_props,
                                               property_parameter='props')
            for batch in chunks(self.node_properties(), size=batch_size):
                graph.run(q, props=list(batch), append_props=self.append_props)

        elif self.preserve and self.append_props:

            q = nodes_merge_unwind_preserve_array_props(self.labels, self.merge_keys, self.append_props, self.preserve,
                                                        property_parameter='props')
            print(q)
            for batch in chunks(self.node_properties(), size=batch_size):
                graph.run(q, props=list(batch), append_props=self.append_props, preserve=self.preserve)

    def node_properties(self):
        """
        Yield properties of the nodes in this set. Used for create function.
        """
        for n in self.nodes:
            yield dict(n)

    def all_properties_in_nodeset(self):
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
        for p in self.all_properties_in_nodeset():
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

    def create_index(self, graph):
        """
        Create indices for all label/merge ky combinations as well as a composite index if multiple merge keys exist.

        In Neo4j 3.x recreation of an index did not raise an error. In Neo4j 4 you cannot create an existing index.

        Index creation syntax changed from Neo4j 3.5 to 4. So far the old syntax is still supported. All py2neo
        functions (v4.4) work on both versions.
        """
        if self.merge_keys:
            for label in self.labels:
                # create individual indexes
                for prop in self.merge_keys:
                    create_single_index(graph, label, prop)

                # composite indexes
                if len(self.merge_keys) > 1:
                    create_composite_index(graph, label, self.merge_keys)
