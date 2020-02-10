# This excample script shows how to download a data file,
# parse nodes and relationships from the file and load them to Neo4j
#
# The maintainer of this package has a background in computational biology.
# This example loads data on gene IDs from a public genome database.
# We create (:Gene) nodes and (:Gene)-[:MAPS]->(:Gene) relationships.
#
# Example line of data file:
# 9606	11	NATP	-	AACP|NATP1	HGNC:HGNC:15	8	8p22	N-acetyltransferase pseudogene	pseudo	NATP	N-acetyltransferase pseudogene	O	arylamide acetylase pseudogene	20191221	-

import gzip
import os
import shutil
from urllib.request import urlopen

from graphio import NodeSet, RelationshipSet
import py2neo


# setup file paths, Neo4j config and Graph instance
DOWNLOAD_DIR = "/set/your/path/here"
DOWNLOAD_FILE_PATH = os.path.join(DOWNLOAD_DIR, 'Homo_sapiens.gene_info.gz')
NEO4J_HOST = 'localhost'
NEO4J_PORT = 7687
NEO4J_USER = 'neo4j'
NEO4J_PASSWORD = 'test'

graph = py2neo.Graph(host=NEO4J_HOST, user=NEO4J_USER, password=NEO4J_PASSWORD)
graph.run("MATCH (a) RETURN a LIMIT 1")


# Download file from NCBI FTP Server
print('Download file from NCBI FTP server')
with urlopen('ftp://ftp.ncbi.nih.gov/gene/DATA/GENE_INFO/Mammalia/Homo_sapiens.gene_info.gz') as r:
    with open(DOWNLOAD_FILE_PATH, 'wb') as f:
        shutil.copyfileobj(r, f)


# define NodeSet and RelationshipSet
ncbi_gene_nodes = NodeSet(['Gene'], ['gene_id'])
ensembl_gene_nodes = NodeSet(['Gene'], ['gene_id'])
gene_mapping_rels = RelationshipSet('MAPS', ['Gene'], ['Gene'], ['gene_id'], ['gene_id'])


# iterate the data file and extract nodes/relationships
print('Iterate file and create nodes/relationships')
# collect mapped ENSEMBL gene IDs to avoid duplicate genes
ensembl_gene_ids_added = set()

with gzip.open(DOWNLOAD_FILE_PATH, 'rt') as file:
    # skip header line
    next(file)
    # iterate file
    for line in file:
        fields = line.strip().split('\t')
        ncbi_gene_id = fields[1]

        # get mapping to ENSEMBL Gene IDs
        mapped_ensembl_gene_ids = []
        # get dbXrefs
        db_xrefs = fields[5]
        for mapped_element in db_xrefs.split('|'):
            if 'Ensembl' in mapped_element:
                ensembl_gene_id = mapped_element.split(':')[1]
                mapped_ensembl_gene_ids.append(ensembl_gene_id)

        # create nodes and relationships
        # add NCBI gene node
        ncbi_gene_nodes.add_node({'gene_id': ncbi_gene_id, 'db': 'ncbi'})
        # add ENSEMBL gene nodes if they not exist already
        for ensembl_gene_id in mapped_ensembl_gene_ids:
            if ensembl_gene_id not in ensembl_gene_ids_added:
                ensembl_gene_nodes.add_node({'gene_id': ensembl_gene_id, 'db': 'ensembl'})
                ensembl_gene_ids_added.add(ensembl_gene_id)

        # add (:Gene)-[:MAPS]->(:Gene) relationship
        for ensembl_gene_id in mapped_ensembl_gene_ids:
            gene_mapping_rels.add_relationship(
                {'gene_id': ncbi_gene_id}, {'gene_id': ensembl_gene_id}, {'db': 'ncbi'}
            )


# load data to Neo4j
print(len(ncbi_gene_nodes.nodes))
print(len(ensembl_gene_nodes.nodes))
print(len(gene_mapping_rels.relationships))

# create index for property 'gene_id' on (Gene) nodes first
print('Create index on Gene nodes')
try:
    graph.schema.create_index('Gene', 'gene_id')
except py2neo.database.ClientError:
    pass

# load data, first nodes then relationships
print('Load data to Neo4j')

ncbi_gene_nodes.create(graph)
ensembl_gene_nodes.create(graph)

gene_mapping_rels.create(graph)
