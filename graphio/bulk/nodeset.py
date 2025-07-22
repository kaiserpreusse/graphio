import logging
from uuid import uuid4

from neo4j import DEFAULT_DATABASE, Driver

from graphio.bulk.query_utils import CypherQuery, merge_clause_with_properties
from graphio.utils import (
    BATCHSIZE,
    chunks,
    create_composite_index,
    create_single_index,
    get_label_string_from_list_of_labels,
)

log = logging.getLogger(__name__)


class NodeSet:
    """
    Container for a set of Nodes with the same labels and the same properties that define uniqueness.
    """

    def __init__(
        self,
        labels: list[str] = None,
        merge_keys: list[str] = None,
        default_props: dict = None,
        preserve: list[str] = None,
        append_props: list[str] = None,
        additional_labels: list[str] = None,
        indexed: bool = False,
    ):
        """ """
        self.labels = labels
        self.merge_keys = merge_keys
        self.default_props = default_props or {}
        self.preserve = preserve or []
        self.append_props = append_props or []
        self.indexed = indexed
        self.additional_labels = additional_labels or []

        self.uuid = str(uuid4())

        self.nodes = []

    def __str__(self):
        return f'<NodeSet ({self.labels}; {self.merge_keys})>'

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

        for other_node_properties in self.nodes:
            this_values = frozenset([other_node_properties[key] for key in self.merge_keys])
            if this_values == compare_values:
                return None

        # add node if not found
        self.add_node(properties)

    @property
    def metadata_dict(self):
        return {'labels': self.labels, 'merge_keys': self.merge_keys}

    def object_file_name(self, suffix: str = None) -> str:
        """
        Create a unique name for this NodeSet that indicates content. Pass an optional suffix.
        NOTE: suffix has to include the '.' for a filename!

            `nodeset_Label_merge-key_uuid`

        With suffix:

            `nodeset_Label_merge-key_uuid.json`
        """
        basename = f'nodeset_{"_".join(self.labels)}_{"_".join(self.merge_keys)}_{self.uuid}'
        if suffix:
            basename += suffix
        return basename

    def create(self, graph: Driver, database: str = DEFAULT_DATABASE, batch_size=None):
        """
        Create all nodes from NodeSet.
        """
        log.debug('Create NodeSet')
        if not batch_size:
            batch_size = BATCHSIZE
        log.debug(f'Batch Size: {batch_size}')

        q = nodes_create_factory(
            self.labels, property_parameter='props', additional_labels=self.additional_labels
        )

        # Define transaction function once
        def create_batch(tx, batch_props):
            result = tx.run(q, props=batch_props)
            result.consume()
            return []

        with graph.session(database=database) as session:
            for batch in chunks(self.nodes, size=batch_size):
                session.execute_write(create_batch, list(batch))

    def merge(
        self,
        graph,
        merge_properties=None,
        batch_size=None,
        preserve=None,
        append_props=None,
        database=None,
    ):
        """
        Merge nodes from NodeSet on merge properties.

        :param merge_properties: The merge properties.
        """
        if not self.labels:
            log.warning('MERGing without labels will not use an index and is slow.')

        if not self.merge_keys:
            raise ValueError('Merge keys are empty, MERGE requires merge keys.')

        # overwrite if preserve is passed
        if preserve:
            self.preserve = preserve
        # overwrite if array_props is passed
        if append_props:
            self.append_props = append_props

        if not batch_size:
            batch_size = BATCHSIZE

        if not merge_properties:
            merge_properties = self.merge_keys

        q = nodes_merge_factory(
            self.labels,
            self.merge_keys,
            array_props=self.append_props,
            preserve=self.preserve,
            property_parameter='props',
            additional_labels=self.additional_labels,
        )

        # Define transaction function once
        def merge_batch(tx, batch_props):
            result = tx.run(
                q, props=batch_props, append_props=self.append_props, preserve=self.preserve
            )
            result.consume()
            return []

        with graph.session(database=database) as session:
            for batch in chunks(self.nodes, size=batch_size):
                session.execute_write(merge_batch, list(batch))

    def all_property_keys(self) -> set[str]:
        """
        Return a set of all property keys in this NodeSet

        :return: A set of unique property keys of a NodeSet
        """
        all_props = set()

        # collect properties
        for props in self.nodes:
            for k in props:
                all_props.add(k)

        return all_props

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


# NodeSet-specific query factories
def nodes_create_factory(labels, property_parameter=None, additional_labels=None):
    if not property_parameter:
        property_parameter = 'props'

    if additional_labels:
        labels = labels + additional_labels

    label_string = get_label_string_from_list_of_labels(labels)

    q = CypherQuery(
        f'UNWIND ${property_parameter} AS properties',
        f'CREATE (n{label_string})',
        'SET n = properties',
    )

    return q.query()


def nodes_merge_factory(
    labels,
    merge_properties,
    array_props=None,
    preserve=None,
    property_parameter=None,
    additional_labels=None,
):
    """
    Generate a :code:`MERGE` query based on the combination of paremeters.
    """
    if not array_props:
        array_props = []

    if not preserve:
        preserve = []

    if not property_parameter:
        property_parameter = 'props'

    on_create_array_props_list = []
    for ap in array_props:
        on_create_array_props_list.append(f'n.{ap} = [properties.{ap}]')
    on_create_array_props_string = ', '.join(on_create_array_props_list)

    on_match_array_props_list = []
    for ap in array_props:
        if ap not in preserve:
            on_match_array_props_list.append(f'n.{ap} = n.{ap} + properties.{ap}')
    on_match_array_props_string = ', '.join(on_match_array_props_list)

    q = CypherQuery()
    # add UNWIND
    q.append(f'UNWIND ${property_parameter} AS properties')
    # add MERGE
    q.append(merge_clause_with_properties(labels, merge_properties))

    # handle different ON CREATE SET and ON MATCH SET cases
    if not array_props and not preserve:
        q.append('ON CREATE SET n = properties')
        q.append('ON MATCH SET n += properties')
    elif not array_props and preserve:
        q.append('ON CREATE SET n = properties')
        q.append('ON MATCH SET n += apoc.map.removeKeys(properties, $preserve)')
    elif array_props and not preserve:
        q.append('ON CREATE SET n = apoc.map.removeKeys(properties, $append_props)')
        q.append(f'ON CREATE SET {on_create_array_props_string}')
        q.append('ON MATCH SET n += apoc.map.removeKeys(properties, $append_props)')
        q.append(f'ON MATCH SET {on_match_array_props_string}')
    elif array_props and preserve:
        q.append('ON CREATE SET n = apoc.map.removeKeys(properties, $append_props)')
        q.append(f'ON CREATE SET {on_create_array_props_string}')
        q.append(
            'ON MATCH SET n += apoc.map.removeKeys(apoc.map.removeKeys(properties, $append_props), $preserve)'
        )
        if on_match_array_props_list:
            q.append(f'ON MATCH SET {on_match_array_props_string}')

    if additional_labels:
        q.append(f'SET n:{":".join(additional_labels)}')

    return q.query()
