"""
OGM-specific query building utilities.
"""


def where_clause_with_properties(
    properties: dict, prop_name: str = None, node_variable: str = None
):
    """
    n.sid = properties.sid
    AND n.name = properties.name
    AND n.age = properties.age

    No leading WHERE!

    :param properties: Dictionary of properties
    :param prop_name: Optional name of the parameter used in the query. Default is 'properties'.
    :param node_variable: Optional name of the node variable. Default is 'n'.
    :return:
    """
    if not prop_name:
        prop_name = 'properties'
    if not node_variable:
        node_variable = 'n'

    where_strings = []
    for k, _ in properties.items():
        where_strings.append(f'{node_variable}.{k} = {prop_name}.{k}')
    where_string = ' AND '.join(where_strings)
    return where_string
