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


def nodes_create_factory(labels, property_parameter=None, additional_labels=None, source=False):
    if not property_parameter:
        property_parameter = 'props'

    if additional_labels:
        labels = labels + additional_labels

    label_string = get_label_string_from_list_of_labels(labels)

    q = CypherQuery(f"UNWIND ${property_parameter} AS properties",
                    f"CREATE (n{label_string})",
                    "SET n = properties")
    if source:
        q.append("SET n._source = [$source]")

    return q.query()


def nodes_merge_factory(labels, merge_properties, array_props=None, preserve=None, property_parameter=None,
                        additional_labels=None, source=False):
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
        on_create_array_props_list.append(f"n.{ap} = [properties.{ap}]")
    on_create_array_props_string = ', '.join(on_create_array_props_list)

    on_match_array_props_list = []
    for ap in array_props:
        if ap not in preserve:
            on_match_array_props_list.append(f"n.{ap} = n.{ap} + properties.{ap}")
    on_match_array_props_string = ', '.join(on_match_array_props_list)

    q = CypherQuery()
    # add UNWIND
    q.append(f"UNWIND ${property_parameter} AS properties")
    # add MERGE
    q.append(merge_clause_with_properties(labels, merge_properties))

    # handle different ON CREATE SET and ON MATCH SET cases
    if not array_props and not preserve:
        q.append("ON CREATE SET n = properties")
        q.append("ON MATCH SET n += properties")
    elif not array_props and preserve:
        q.append("ON CREATE SET n = properties")
        q.append("ON MATCH SET n += apoc.map.removeKeys(properties, $preserve)")
    elif array_props and not preserve:
        q.append("ON CREATE SET n = apoc.map.removeKeys(properties, $append_props)")
        q.append(f"ON CREATE SET {on_create_array_props_string}")
        q.append("ON MATCH SET n += apoc.map.removeKeys(properties, $append_props)")
        q.append(f"ON MATCH SET {on_match_array_props_string}")
    elif array_props and preserve:
        q.append("ON CREATE SET n = apoc.map.removeKeys(properties, $append_props)")
        q.append(f"ON CREATE SET {on_create_array_props_string}")
        q.append("ON MATCH SET n += apoc.map.removeKeys(apoc.map.removeKeys(properties, $append_props), $preserve)")
        if on_match_array_props_list:
            q.append(f"ON MATCH SET {on_match_array_props_string}")

    if source:
        q.append(f"ON CREATE SET n._source = [$source]")
        q.append(f"ON MATCH SET n._source = n._source + [$source]")

    if additional_labels:
        q.append(f"SET n:{':'.join(additional_labels)}")


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
