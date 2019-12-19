def nodes_create_unwind(labels):
    """

    UNWIND $props AS properties CREATE (n:Gene) SET n = properties

    Call with:

        {'props': [{'sid': 1}, {'sid': 2}, ...]}

    :param labels: Labels for the create query.
    :type labels: list[str]
    :return: Query
    """
    return "UNWIND $props AS properties CREATE (n:{}) SET n = properties".format(":".join(labels))


def query_merge_nodes_unwind(labels, merge_properties):
    """
    Generate a MERGE query which uses the uniqueness properties defined by the parser output.

        UNWIND $props AS properties
        MERGE (n:Gene {properties.sid: '1234'})
        ON CREATE SET n = properties
        ON MATCH SET n = properties

    Call with:

        {'props': [{'sid': 1}, {'sid': 2}, ...]}

    :param labels: Labels for the query.
    :type labels: list[str]
    :param merge_properties: Unique properties for the node.
    :type merge_properties: list[str]
    :return: Query
    """

    label_string = ':'.join(labels)

    merge_strings = []
    for u in merge_properties:
        merge_strings.append("{0}: properties.{0}".format(u))

    merge_string = ', '.join(merge_strings)

    q = "UNWIND $props AS properties \n" \
        "MERGE (n:{0} {{ {1} }} ) \n" \
        "ON CREATE SET n = properties \n" \
        "ON MATCH SET n += properties".format(label_string, merge_string)

    return q


def query_create_rels_unwind_from_relationship(relationship, property_identifier=None):
    """

    UNWIND { rels } AS rel
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

    return query_create_rels_unwind(relationship.start_node_labels, relationship.end_node_labels,
                                    relationship.start_node_properties, relationship.end_node_properties,
                                    relationship.rel_type)


def query_create_rels_unwind(start_node_labels, end_node_labels, start_node_properties,
                             end_node_properties, rel_type, property_identifier=None):
    """
    Create relationship query with explicit arguments (i.e. extracted from a mongoDB document) and not from
    a Relationship object. This is used in cases where we avoid recreating Relationship objects from mongoDB
    documents.

    UNWIND { rels } AS rel
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

    start_node_label_string = ':'.join(start_node_labels)
    end_node_label_string = ':'.join(end_node_labels)

    q = "UNWIND {{ {0} }} AS rel \n".format(property_identifier)
    q += "MATCH (a:{0}), (b:{1}) \n".format(start_node_label_string, end_node_label_string)

    # collect WHERE clauses
    where_clauses = []
    for property in start_node_properties:
        where_clauses.append('a.{0} = rel.start_{0}'.format(property))
    for property in end_node_properties:
        where_clauses.append('b.{0} = rel.end_{0}'.format(property))

    q += "WHERE " + ' AND '.join(where_clauses) + " \n"

    q += "CREATE (a)-[r:{0}]->(b) \n".format(rel_type)
    q += "SET r = rel.properties RETURN count(r)\n"

    return q
