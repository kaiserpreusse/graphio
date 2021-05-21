from typing import List


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


def merge_clause_merge_properties(merge_properties):
    """
    {sid: properties.sid}

    :param merge_properties: The merge properties
    :return:
    """
    merge_strings = []
    for u in merge_properties:
        merge_strings.append("{0}: properties.{0}".format(u))
    merge_string = ', '.join(merge_strings)
    return merge_string


def merge_clause(labels, merge_properties):
    label_string = ':'.join(labels)
    return f"MERGE (n:{label_string} {{ {merge_clause_merge_properties(merge_properties)} }} )"


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
                    merge_clause(labels, merge_properties),
                    "ON CREATE SET n = properties",
                    "ON MATCH SET n += apoc.map.removeKeys(properties, $preserve)")

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
                    merge_clause(labels, merge_properties),
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
                    merge_clause(labels, merge_properties),
                    "ON CREATE SET n = apoc.map.removeKeys(properties, $append_props)",
                    f"ON CREATE SET {on_create_array_props_string}",
                    "ON MATCH SET n += apoc.map.removeKeys(apoc.map.removeKeys(properties, $append_props), $preserve)")

    if on_match_array_props_list:
        q.append(f"ON MATCH SET {on_match_array_props_string}")

    return q.query()
