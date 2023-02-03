from typing import List, Union
from graphio.objects.properties import ArrayProperty


def get_label_string_from_list_of_labels(labels: Union[List[str], None]) -> str:
    """
    Get a label string from a list of labels. If the list is empty, return an empty string.
    :param labels: List of labels
    :return: Label string
    """
    if labels:
        return f":{':'.join(labels)}"
    else:
        return ""


class CypherQuery:

    def __init__(self, *statements):
        self._statements = []
        for s in statements:
            self._statements.append(s)

    def query(self):
        return '\n'.join(
            [s for s in self._statements if s]
        )

    def append(self, value):
        self._statements.append(value)


def match_properties_as_string(merge_properties, prop_name):
    """
    sid: properties.sid

    :param merge_properties: The merge properties
    :return:
    """
    merge_strings = []
    for u in merge_properties:
        merge_strings.append(f"{u}: {prop_name}.{u}")
    merge_string = ', '.join(merge_strings)
    return merge_string


def merge_clause_with_properties(labels: List[str], merge_properties: List[str], prop_name=None, node_variable=None):
    """
    MERGE (n:Node {properties.sid: '1234'})

    :param labels: Labels for the match query.
    :param merge_properties: The merge properties
    :param prop_name: Optional name of the parameter used in the query. Default is 'properties'.
    :param node_variable: Optional name of the node variable. Default is 'n'.
    :return: Query
    """
    if not prop_name:
        prop_name = 'properties'
    if not node_variable:
        node_variable = 'n'

    label_string = get_label_string_from_list_of_labels(labels)
    return f"MERGE ({node_variable}{label_string} {{ {match_properties_as_string(merge_properties, prop_name)} }} )"


def match_clause_with_properties(labels: List[str], merge_properties: List[str], prop_name=None, node_variable=None):
    """
    MATCH (n:Node {properties.sid: '1234'})

    :param labels: Labels for the match query.
    :param merge_properties: The merge properties
    :param prop_name: Optional name of the parameter used in the query. Default is 'properties'.
    :param node_variable: Optional name of the node variable. Default is 'n'.
    :return: Query
    """
    if not prop_name:
        prop_name = 'properties'
    if not node_variable:
        node_variable = 'n'

    label_string = get_label_string_from_list_of_labels(labels)
    return f"MATCH ({node_variable}{label_string} {{ {match_properties_as_string(merge_properties, prop_name)} }} )"


def nodes_create_unwind(labels, property_parameter=None):
    """
    Generate a :code:`CREATE` query using :code:`UNWIND` for batch creation of nodes.::

        UNWIND $props AS properties CREATE (n:Gene) SET n = properties

    Pass the node properties as parameter to the query, e.g. with a :py:obj:`py2neo.Graph`::

        graph.run(query, props=[{'id': 1}, {'id': 2}, ...])

    You can optionally set the name of the parameter with the argument :code:`property_parameter`::

        query = nodes_create_unwind(['Foo'], query_parameter='mynodes')

        graph.run(query, mynodes=[{'id': 1}, {'id': 2}, ...])


    :param labels: Labels for the create query.
    :type labels: list[str]
    :param property_parameter: Optional name of the parameter used in the query. Default is 'props'.
    :type property_parameter: str
    :return: Query
    """
    if not property_parameter:
        property_parameter = 'props'

    label_string = get_label_string_from_list_of_labels(labels)

    q = CypherQuery(f"UNWIND ${property_parameter} AS properties",
                    f"CREATE (n{label_string})",
                    "SET n = properties")

    return q.query()


def nodes_merge_unwind_preserve(labels, merge_properties, property_parameter=None):
    """
    Generate a :code:`MERGE` query which uses defined properties to :code:`MERGE` upon::

        UNWIND $props AS properties
        MERGE (n:Node {properties.sid: '1234'})
        ON CREATE SET n = properties
        ON MATCH SET n += apoc.map.removeKeys(properties, $preserve)

    The '+=' operator in ON MATCH updates the node properties provided and leaves the others untouched.
    """
    if not property_parameter:
        property_parameter = 'props'

    q = CypherQuery(f"UNWIND ${property_parameter} AS properties",
                    merge_clause_with_properties(labels, merge_properties),
                    "ON CREATE SET n = properties",
                    "ON MATCH SET n += apoc.map.removeKeys(properties, $preserve)")

    return q.query()


def nodes_merge_unwind(labels, merge_properties, property_parameter=None):
    """
    Generate a :code:`MERGE` query which uses defined properties to :code:`MERGE` upon::

        UNWIND $props AS properties
        MERGE (n:Node {properties.sid: '1234'})
        ON CREATE SET n = properties
        ON MATCH SET n += properties

    The '+=' operator in ON MATCH updates the node properties provided and leaves the others untouched.

    Call with the labels and a list of properties to :code:`MERGE` on::

        query = nodes_merge_unwind(['Foo', 'Bar'], ['node_id'])

    Pass the node properties as parameter to the query, e.g. with a :py:obj:`py2neo.Graph`::

        graph.run(query, props=[{'node_id': 1}, {'node_id': 2}, ...])

    You can optionally set the name of the parameter with the argument :code:`property_parameter`::

        query = nodes_merge_unwind([['Foo', 'Bar'], ['node_id'], query_parameter='mynodes')

        graph.run(query, mynodes=[{'node_id': 1}, {'node_id': 2}, ...])


    :param labels: Labels for the query.
    :type labels: list[str]
    :param merge_properties: Unique properties for the node.
    :type merge_properties: list[str]
    :param property_parameter: Optional name of the parameter used in the query. Default is 'props'.
    :type property_parameter: str
    :return: Query
    """
    if not property_parameter:
        property_parameter = 'props'

    label_string = get_label_string_from_list_of_labels(labels)

    merge_strings = []
    for u in merge_properties:
        merge_strings.append("{0}: properties.{0}".format(u))

    merge_string = ', '.join(merge_strings)

    q = CypherQuery(f"UNWIND ${property_parameter} AS properties",
                    f"MERGE (n{label_string} {{ {merge_string} }} )",
                    "ON CREATE SET n = properties",
                    "ON MATCH SET n += properties")

    return q.query()


def nodes_merge_unwind_array_props(labels, merge_properties, array_props, property_parameter=None):
    """
    Generate a :code:`MERGE` query which uses defined properties to :code:`MERGE` upon::

        UNWIND $props AS properties
        MERGE (n:Node {properties.sid: '1234'})
        ON CREATE SET n = properties
        ON MATCH SET n += apoc.map.removeKeys(properties, $preserve)

    The '+=' operator in ON MATCH updates the node properties provided and leaves the others untouched.
    """
    if not property_parameter:
        property_parameter = 'props'

    on_create_array_props_list = []
    for ap in array_props:
        on_create_array_props_list.append(f"n.{ap} = [properties.{ap}]")
    on_create_array_props_string = ', '.join(on_create_array_props_list)

    on_match_array_props_list = []
    for ap in array_props:
        on_match_array_props_list.append(f"n.{ap} = n.{ap} + properties.{ap}")
    on_match_array_props_string = ', '.join(on_match_array_props_list)

    q = CypherQuery(f"UNWIND ${property_parameter} AS properties",
                    merge_clause_with_properties(labels, merge_properties),
                    "ON CREATE SET n = apoc.map.removeKeys(properties, $append_props)",
                    f"ON CREATE SET {on_create_array_props_string}",
                    "ON MATCH SET n += apoc.map.removeKeys(properties, $append_props)",
                    f"ON MATCH SET {on_match_array_props_string}")

    return q.query()


def nodes_merge_unwind_preserve_array_props(labels, merge_properties, array_props, preserve, property_parameter=None):
    """
    Generate a :code:`MERGE` query which uses defined properties to :code:`MERGE` upon::

        UNWIND $props AS properties
        MERGE (n:Person { name: properties.name } )
        ON CREATE SET n = apoc.map.removeKeys(properties, $append_props)
        ON CREATE SET n.foo = [properties.foo], n.bar = [properties.bar]
        ON MATCH SET n += apoc.map.removeKeys(apoc.map.removeKeys(properties, $append_props), $preserve)
        ON MATCH SET n.foo = n.foo + properties.foo"
    """
    if not property_parameter:
        property_parameter = 'props'

    on_create_array_props_list = []
    for ap in array_props:
        on_create_array_props_list.append(f"n.{ap} = [properties.{ap}]")
    on_create_array_props_string = ', '.join(on_create_array_props_list)

    on_match_array_props_list = []
    for ap in array_props:
        if ap not in preserve:
            on_match_array_props_list.append(f"n.{ap} = n.{ap} + properties.{ap}")
    on_match_array_props_string = ', '.join(on_match_array_props_list)

    q = CypherQuery(f"UNWIND ${property_parameter} AS properties",
                    merge_clause_with_properties(labels, merge_properties),
                    "ON CREATE SET n = apoc.map.removeKeys(properties, $append_props)",
                    f"ON CREATE SET {on_create_array_props_string}",
                    "ON MATCH SET n += apoc.map.removeKeys(apoc.map.removeKeys(properties, $append_props), $preserve)")

    if on_match_array_props_list:
        q.append(f"ON MATCH SET {on_match_array_props_string}")

    return q.query()


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
            d['start_{}'.format(k)] = v
        for k, v in r[1].items():
            d['end_{}'.format(k)] = v
        d['properties'] = r[2]
        output.append(d)

    return {property_identifier: output}


def rels_create_unwind(start_node_labels, end_node_labels, start_node_properties,
                             end_node_properties, rel_type, property_identifier=None):
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
    q.append(f"UNWIND ${property_identifier} AS rel")
    q.append(f"MATCH (a{start_node_label_string}), (b{end_node_label_string})")

    # collect WHERE clauses
    where_clauses = []
    for property in start_node_properties:
        if isinstance(property, ArrayProperty):
            where_clauses.append(f'rel.start_{property} IN a.{property}')
        else:
            where_clauses.append('a.{0} = rel.start_{0}'.format(property))
    for property in end_node_properties:
        if isinstance(property, ArrayProperty):
            where_clauses.append(f'rel.end_{property} IN b.{property}')
        else:
            where_clauses.append('b.{0} = rel.end_{0}'.format(property))

    q.append("WHERE " + ' AND '.join(where_clauses))

    q.append(f"CREATE (a)-[r:{rel_type}]->(b)")
    q.append("SET r = rel.properties RETURN count(r)")

    return q.query()


def rels_merge_unwind(start_node_labels, end_node_labels, start_node_properties,
                             end_node_properties, rel_type, property_identifier=None):
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
    q.append(f"UNWIND ${property_identifier} AS rel")
    q.append(f"MATCH (a{start_node_label_string}), (b{end_node_label_string})")

    # collect WHERE clauses
    where_clauses = []
    for property in start_node_properties:
        if isinstance(property, ArrayProperty):
            where_clauses.append(f'rel.start_{property} IN a.{property}')
        else:
            where_clauses.append('a.{0} = rel.start_{0}'.format(property))
    for property in end_node_properties:
        if isinstance(property, ArrayProperty):
            where_clauses.append(f'rel.end_{property} IN b.{property}')
        else:
            where_clauses.append('b.{0} = rel.end_{0}'.format(property))

    q.append("WHERE " + ' AND '.join(where_clauses))

    q.append(f"MERGE (a)-[r:{rel_type}]->(b)")
    q.append("SET r = rel.properties RETURN count(r)")

    return q.query()
