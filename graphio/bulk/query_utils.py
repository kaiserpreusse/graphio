"""
Bulk-specific query building utilities shared between NodeSet and RelationshipSet.
"""

from graphio.utils import get_label_string_from_list_of_labels


class Property:
    def __init__(self, key: str):
        self.key = key

    def __str__(self):
        return self.key


class ArrayProperty(Property):
    def __init__(self, key: str):
        super().__init__(key)


class CypherQuery:
    def __init__(self, *statements):
        self._statements = []
        for s in statements:
            self._statements.append(s)

    def query(self):
        return '\n'.join([s for s in self._statements if s])

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
        merge_strings.append(f'{u}: {prop_name}.{u}')
    merge_string = ', '.join(merge_strings)
    return merge_string


def merge_clause_with_properties(
    labels: list[str], merge_properties: list[str], prop_name=None, node_variable=None
):
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
    return f'MERGE ({node_variable}{label_string} {{ {match_properties_as_string(merge_properties, prop_name)} }} )'


def match_clause_with_properties(
    labels: list[str], merge_properties: list[str], prop_name=None, node_variable=None
):
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
    return f'MATCH ({node_variable}{label_string} {{ {match_properties_as_string(merge_properties, prop_name)} }} )'
