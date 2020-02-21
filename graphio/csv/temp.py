import csv
import os
from uuid import uuid4
import logging
from pymongo import MongoClient



log = logging.getLogger(__name__)


def nodeset_to_csv(filepath, filename, nodeset):
    """

    :param filepath:
    :param nodeset: The Nodeset
    :type nodeset: graphio.NodeSet
    :return:
    """
    all_props = nodeset.all_properties_in_nodeset()

    with open(os.path.join(filepath, filename), 'w', newline='') as csvfile:
        fieldnames = list(all_props)
        writer = csv.DictWriter(csvfile, fieldnames=fieldnames)  # , quoting=csv.QUOTE_ALL)

        writer.writeheader()

        for n in nodeset.nodes:
            writer.writerow(dict(n))


def update_from_mongo_to_csv(filepath, csv_filename, cypher_filename, mongo_url, update):
    # mongoDB
    if mongo_url:
        if isinstance(mongo_url, MongoClient):
            client = mongo_url
        else:
            client = MongoClient(mongo_url)
    else:
        client = MongoClient('mongodb://localhost:27017/')

    db = client['cellmap']

    object_collection = db[update.uuid]

    # figure out if nodeset or relset
    if update.node_sets:
        # prepend 'node_' to csv_filename
        csv_filename = 'node_{}'.format(csv_filename)
        cypher_filename = 'node_{}'.format(cypher_filename)
        ref_nodeset = list(update.node_sets)[0]
        ref_nodeset.nodes = []
        ref_nodeset.batch_size = 1000
        # get nodes from mongodb and store in nodeset

        for batch in iterate_collection_in_batch(object_collection):
            for d in batch:
                d['update_uuid'] = update.uuid
                ref_nodeset.add_node(d)

        nodeset_to_csv(filepath, csv_filename, ref_nodeset)

        query = nodeset_create_csv_query(csv_filename, ref_nodeset)

    if update.rel_sets:
        cypher_index_filename = 'index_{}'.format(cypher_filename)
        csv_filename = 'rel_{}'.format(csv_filename)
        cypher_filename = 'rel_{}'.format(cypher_filename)

        ref_relset = list(update.rel_sets)[0]
        ref_relset.relationships = []
        ref_relset.unique = False
        ref_relset.batch_size = 1000
        # get nodes from mongodb and store in nodeset

        for batch in iterate_collection_in_batch(object_collection):
            for d in batch:
                rel_props = d['properties']
                rel_props['update_uuid'] = update.uuid

                ref_relset.add_relationship(d['start_node_properties'], d['end_node_properties'], rel_props)

        relset_to_csv(filepath, csv_filename, ref_relset)

        query = relset_create_csv_query(csv_filename, ref_relset)

        index_queries = relset_create_index_query(ref_relset)
        with open(os.path.join(filepath, cypher_index_filename), 'wt') as f:
            for index_query in index_queries:
                f.write(index_query + ";\n")

    with open(os.path.join(filepath, cypher_filename), 'wt') as f:
        f.write(query + ";\n")


def nodeset_create_csv_query(filename, nodeset, periodic_commit=1000):
    """

    UNWIND $props AS properties CREATE (n:Gene) SET n = properties

    Call with:

        {'props': [{'sid': 1}, {'sid': 2}, ...]}

    :param labels: Labels for the create query.
    :type labels: list[str]
    :return: Query
    """
    q = "USING PERIODIC COMMIT {} \n".format(periodic_commit)
    q += "LOAD CSV WITH HEADERS FROM 'file:///{}' AS line \n".format(filename)
    q += "CREATE (n:{}) \n".format(':'.join(nodeset.labels))

    props_list = []
    for k in nodeset.all_properties_in_nodeset():
        props_list.append("n.{0} = line.{1}".format(k, k))

    q += "SET {}".format(', '.join(props_list))

    return q


def relset_to_csv(filepath, filename, relset):
    """
    LOAD CSV WITH HEADERS FROM xyz AS line
    MATCH (a:Gene), (b:GeneSymbol)
    WHERE a.sid = line.a_sid AND b.sid = line.b_sid AND b.taxid = line.b_taxid
    CREATE (a)-[r:MAPS]->(b)
    SET r.key1 = line.rel_key1, r.key2 = line.rel_key2

    # CSV file header
    a_sid, b_sid, b_taxid, rel_key1, rel_key2

    :param filepath: Path to csv file.
    :param relset: The RelationshipSet
    :type relset: graphio.RelationshipSet
    """
    header = []

    for prop in relset.start_node_properties:
        header.append("a_{}".format(prop))

    for prop in relset.end_node_properties:
        header.append("b_{}".format(prop))

    for prop in relset.relationships[0].properties:
        header.append("rel_{}".format(prop))

    with open(os.path.join(filepath, filename), 'w', newline='') as csvfile:
        writer = csv.DictWriter(csvfile, fieldnames=header)  # , quoting=csv.QUOTE_ALL)

        writer.writeheader()

        for rel in relset.relationships:
            # create data for row
            rel_csv_dict = {}
            for k, v in rel.start_node_properties.items():
                rel_csv_dict["a_{}".format(k)] = v
            for k, v in rel.end_node_properties.items():
                rel_csv_dict["b_{}".format(k)] = v

            for k, v in rel.properties.items():
                rel_csv_dict["rel_{}".format(k)] = v

            writer.writerow(rel_csv_dict)


def relset_create_csv_query(filename, relset, periodic_commit=1000):
    """

    MATCH (a:Gene), (b:GeneSymbol)
    WHERE a.sid = rel.start_sid AND b.sid = rel.end_sid AND b.taxid = rel.end_taxid
    CREATE (a)-[r:MAPS]->(b)
    SET r = rel.properties

    LOAD CSV WITH HEADERS FROM xyz AS line
    MATCH (a:Gene), (b:GeneSymbol)
    WHERE a.sid = line.a_sid AND b.sid = line.b_sid AND b.taxid = line.b_taxid
    CREATE (a)-[r:MAPS]->(b)
    SET r.key1 = line.rel_key1, r.key2 = line.rel_key2

    :param filepath:
    :param relset:
    :type relset: graphio.RelationshipSet
    :return:
    """
    q = "USING PERIODIC COMMIT {} \n".format(periodic_commit)
    q += "LOAD CSV WITH HEADERS FROM 'file:///{}' AS line \n".format(filename)
    q += "MATCH (a:{0}), (b:{1}) \n".format(':'.join(relset.start_node_labels), ':'.join(relset.end_node_labels))

    where_clauses = []
    for prop in relset.start_node_properties:
        where_clauses.append("a.{0} = line.a_{0}".format(prop))

    for prop in relset.end_node_properties:
        where_clauses.append("b.{0} = line.b_{0}".format(prop))

    q += "WHERE {} \n".format(" AND ".join(where_clauses))

    q += "CREATE (a)-[r:{0}]->(b) \n".format(relset.rel_type)

    rel_prop_list = []
    for prop in relset.relationships[0].properties:
        rel_prop_list.append("r.{0} = line.rel_{0}".format(prop))

    q += "SET {}".format(", ".join(rel_prop_list))

    return q


def relset_create_index_query(relset):
    """
    Extract the startnode/endnode properties from the relset and formulate a
    CREATE INDEX query.

    :param relset: The relset.
    :return: A list of the CREATE INDEX queries.
    """
    exclude_labels = ['LATEST']

    index_queries = []

    for start_node_label in relset.start_node_labels:
        if start_node_label not in exclude_labels:
            for prop in relset.start_node_properties:
                q = "CREATE INDEX ON :{0}({1})".format(start_node_label, prop)
                index_queries.append(q)

    for end_node_label in relset.end_node_labels:
        if end_node_label not in exclude_labels:
            for prop in relset.end_node_properties:
                q = "CREATE INDEX ON :{0}({1})".format(end_node_label, prop)
                index_queries.append(q)

    return index_queries


def print_queries(filepath, *queries):
    filename = os.path.join(filepath, 'queries.cypher')

    with open(filename, 'wt') as f:
        for q in queries:
            f.write(q + "\n\n")
