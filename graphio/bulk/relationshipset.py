import logging
from uuid import uuid4

from graphio.bulk.query_utils import ArrayProperty, CypherQuery
from graphio.utils import (
    BATCHSIZE,
    chunks,
    create_composite_index,
    create_single_index,
    get_label_string_from_list_of_labels,
)

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
    output = ()
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

    def __init__(
        self,
        rel_type: str,
        start_node_labels: list[str],
        end_node_labels: list[str],
        start_node_properties: list[str],
        end_node_properties: list[str],
        default_props: dict = None,
    ):
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
            log.warning(
                'Creating or merging relationships without start node labels is slow because no index is used.'
            )
        if not self.end_node_labels:
            log.warning(
                'Creating or merging relationships without end node labels is slow because no index is used.'
            )
        self.start_node_properties = start_node_properties
        self.end_node_properties = end_node_properties
        self.default_props = default_props

        self.fixed_order_start_node_properties = tuple(self.start_node_properties)
        self.fixed_order_end_node_properties = tuple(self.end_node_properties)

        self.uuid = str(uuid4())
        self.combined = f'{self.rel_type}_{"_".join(sorted(self.start_node_labels))}_{"_".join(sorted(self.end_node_labels))}_{"_".join(sorted([str(x) for x in self.start_node_properties]))}_{"_".join(sorted([str(x) for x in self.end_node_properties]))}'

        self.relationships = []

        self.unique = False
        self.unique_rels = set()

    def __str__(self):
        return f'<RelationshipSet ({self.start_node_labels}; {self.start_node_properties})-[{self.rel_type}]->({self.end_node_labels}; {self.end_node_properties})>'

    def add_relationship(self, start_node_properties, end_node_properties, properties: dict = None):
        """
        Add a relationship to this RelationshipSet.

        :param start_node_properties: Start node properties as dict or OGM instance.
        :param end_node_properties: End node properties as dict or OGM instance.
        :param properties: Relationship properties.
        """
        # Handle OGM instances for start node
        if hasattr(start_node_properties, 'match_dict'):
            start_props = start_node_properties.match_dict
        elif hasattr(start_node_properties, 'model_dump'):
            # Fallback to full properties if match_dict not available
            start_props = start_node_properties.model_dump()
        elif hasattr(start_node_properties, 'dict'):
            start_props = start_node_properties.dict()
        else:
            start_props = start_node_properties  # Regular dict

        # Handle OGM instances for end node
        if hasattr(end_node_properties, 'match_dict'):
            end_props = end_node_properties.match_dict
        elif hasattr(end_node_properties, 'model_dump'):
            end_props = end_node_properties.model_dump()
        elif hasattr(end_node_properties, 'dict'):
            end_props = end_node_properties.dict()
        else:
            end_props = end_node_properties  # Regular dict

        if not properties:
            properties = {}
        if self.default_props:
            rel_props = {**self.default_props, **properties}
        else:
            rel_props = properties

        if self.unique:
            # construct a check set with start_props (values), end_props (values) and properties (values)
            check_set = frozenset(
                list(start_props.values()) + list(end_props.values()) + list(rel_props.values())
            )

            if check_set not in self.unique_rels:
                self.relationships.append((start_props, end_props, rel_props))
                self.unique_rels.add(check_set)
        else:
            self.relationships.append((start_props, end_props, rel_props))

    def add(self, start_node_properties, end_node_properties, properties: dict = None):
        """
        Add a relationship to this RelationshipSet (alias for add_relationship).

        :param start_node_properties: Start node properties as dict or OGM instance.
        :param end_node_properties: End node properties as dict or OGM instance.
        :param properties: Relationship properties.
        """
        return self.add_relationship(start_node_properties, end_node_properties, properties)

    def all_property_keys(self) -> set[str]:
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
        return {
            'rel_type': self.rel_type,
            'start_node_labels': self.start_node_labels,
            'end_node_labels': self.end_node_labels,
            'start_node_properties': self.start_node_properties,
            'end_node_properties': self.end_node_properties,
        }

    def object_file_name(self, suffix: str = None) -> str:
        """
        Create a unique name for this RelationshipSet that indicates content. Pass an optional suffix.
        NOTE: suffix has to include the '.' for a filename!

            `relationshipset_StartLabel_TYPE_EndLabel_uuid`

        With suffix:

            `relationshipset_StartLabel_TYPE_EndLabel_uuid.json`
        """
        basename = f'relationshipset_{"_".join(self.start_node_labels)}_{self.rel_type}_{"_".join(self.end_node_labels)}_{self.uuid}'
        if suffix:
            basename += suffix
        return basename

    def create(self, graph, database=None, batch_size=None):
        """
        Create relationships in this RelationshipSet
        """
        log.debug('Create RelationshipSet')
        if not batch_size:
            batch_size = BATCHSIZE

        # iterate over chunks of rels
        q = rels_create_factory(
            self.start_node_labels,
            self.end_node_labels,
            self.start_node_properties,
            self.end_node_properties,
            self.rel_type,
        )

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
            batch_size = BATCHSIZE
        log.debug(f'Batch Size: {batch_size}')

        # iterate over chunks of rels
        q = rels_merge_factory(
            self.start_node_labels,
            self.end_node_labels,
            self.start_node_properties,
            self.end_node_properties,
            self.rel_type,
        )

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


# RelationshipSet-specific query factories
def rels_params_from_objects(relationships, property_identifier=None):
    """
    Format Relationship properties into a one level dictionary matching the query generated in
    `query_create_rels_from_list`. This is necessary because you cannot access nested dictionairies
    in the UNWIND query.

    UNWIND { rels } AS rel
    MATCH (a:Gene), (b:GeneSymbol)
    WHERE a.sid = rel.start_sid AND b.sid = rel.end_sid AND b.taxid = rel.end_taxid
    CREATE (a)-[r:MAPS]->(b)
    SET r = rel.properties

    Call with params:
        {'start_sid': 1, 'end_sid': 2, 'end_taxid': '9606', 'properties': {'foo': 'bar} }

    :param relationships: List of Relationships.
    :return: List of parameter dictionaries.
    """
    if not property_identifier:
        property_identifier = 'rels'

    output = []

    for r in relationships:
        d = {}
        for k, v in r[0].items():
            d[f'start_{k}'] = v
        for k, v in r[1].items():
            d[f'end_{k}'] = v
        d['properties'] = r[2]
        output.append(d)

    return {property_identifier: output}


def rels_create_factory(
    start_node_labels,
    end_node_labels,
    start_node_properties,
    end_node_properties,
    rel_type,
    property_identifier=None,
):
    """
    Create relationship query with explicit arguments.

    UNWIND $rels AS rel
    MATCH (a:Gene), (b:GeneSymbol)
    WHERE a.sid = rel.start_sid AND b.sid = rel.end_sid AND b.taxid = rel.end_taxid
    CREATE (a)-[r:MAPS]->(b)
    SET r = rel.properties

    Call with params:
        {'start_sid': 1, 'end_sid': 2, 'end_taxid': '9606', 'properties': {'foo': 'bar} }

    Within UNWIND you cannot access nested dictionaries such as 'rel.start_node.sid'. Thus the
    parameters are created in a separate function.

    :param relationship: A Relationship object to create the query.
    :param property_identifier: The variable used in UNWIND.
    :return: Query
    """

    if not property_identifier:
        property_identifier = 'rels'

    start_node_label_string = get_label_string_from_list_of_labels(start_node_labels)
    end_node_label_string = get_label_string_from_list_of_labels(end_node_labels)

    q = CypherQuery()
    q.append(f'UNWIND ${property_identifier} AS rel')
    q.append(f'MATCH (a{start_node_label_string}), (b{end_node_label_string})')

    # collect WHERE clauses
    where_clauses = []
    for property in start_node_properties:
        if isinstance(property, ArrayProperty):
            where_clauses.append(f'rel.start_{property} IN a.{property}')
        else:
            where_clauses.append(f'a.{property} = rel.start_{property}')
    for property in end_node_properties:
        if isinstance(property, ArrayProperty):
            where_clauses.append(f'rel.end_{property} IN b.{property}')
        else:
            where_clauses.append(f'b.{property} = rel.end_{property}')

    q.append('WHERE ' + ' AND '.join(where_clauses))

    q.append(f'CREATE (a)-[r:{rel_type}]->(b)')
    q.append('SET r = rel.properties')

    return q.query()


def rels_merge_factory(
    start_node_labels,
    end_node_labels,
    start_node_properties,
    end_node_properties,
    rel_type,
    property_identifier=None,
):
    """
    Merge relationship query with explicit arguments.

    Note: The MERGE on relationships does not take relationship properties into account!

    UNWIND $rels AS rel
    MATCH (a:Gene), (b:GeneSymbol)
    WHERE a.sid = rel.start_sid AND b.sid = rel.end_sid AND b.taxid = rel.end_taxid
    MERGE (a)-[r:MAPS]->(b)
    SET r = rel.properties

    Call with params:
        {'start_sid': 1, 'end_sid': 2, 'end_taxid': '9606', 'properties': {'foo': 'bar} }

    Within UNWIND you cannot access nested dictionaries such as 'rel.start_node.sid'. Thus the
    parameters are created in a separate function.

    :param relationship: A Relationship object to create the query.
    :param property_identifier: The variable used in UNWIND.
    :return: Query
    """

    if not property_identifier:
        property_identifier = 'rels'

    start_node_label_string = get_label_string_from_list_of_labels(start_node_labels)
    end_node_label_string = get_label_string_from_list_of_labels(end_node_labels)

    q = CypherQuery()
    q.append(f'UNWIND ${property_identifier} AS rel')
    q.append(f'MATCH (a{start_node_label_string}), (b{end_node_label_string})')

    # collect WHERE clauses
    where_clauses = []
    for property in start_node_properties:
        if isinstance(property, ArrayProperty):
            where_clauses.append(f'rel.start_{property} IN a.{property}')
        else:
            where_clauses.append(f'a.{property} = rel.start_{property}')
    for property in end_node_properties:
        if isinstance(property, ArrayProperty):
            where_clauses.append(f'rel.end_{property} IN b.{property}')
        else:
            where_clauses.append(f'b.{property} = rel.end_{property}')

    q.append('WHERE ' + ' AND '.join(where_clauses))

    q.append(f'MERGE (a)-[r:{rel_type}]->(b)')
    q.append('ON CREATE SET r = rel.properties')
    q.append('ON MATCH SET r += rel.properties')

    return q.query()
