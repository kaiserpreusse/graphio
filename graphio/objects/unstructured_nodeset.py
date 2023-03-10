from typing import List

from pydantic import BaseModel
from neo4j import Driver, Transaction

from graphio.queries import CypherQuery
from graphio.helper import chunks, create_single_index, create_composite_index
from graphio.queries import merge_clause_with_properties


class Node(BaseModel):
    labels: List[str]
    merge_keys: List[str]
    properties: dict = {}


class UnstructuredNodeSet(BaseModel):
    """
    A set of nodes that do not have the same labels/merge keys.
    """
    nodes: List[Node] = []

    def add_node(self, node: Node):
        self.nodes.append(node)

    @property
    def unique_node_definitions(self):
        """
        Return a unique list of label/merge key combinations.
        """
        unique_nodes = set()

        for node in self.nodes:
            node_def = (tuple(node.labels), tuple(node.merge_keys))
            if node_def not in unique_nodes:
                unique_nodes.add(node_def)

        return unique_nodes

    def create_index(self, driver: Driver, database: str = None):
        for labels, merge_keys in self.unique_node_definitions:
            for label in labels:
                for merge_key in merge_keys:
                    create_single_index(driver, label, merge_key, database=database)
                if len(merge_keys) > 1:
                    create_composite_index(driver, label, merge_keys, database=database)

    @staticmethod
    def create_nodes(tx, nodes: List[Node]):
        for node in nodes:
            q = CypherQuery(
                f"CREATE (n:{':'.join(node.labels)})",
                "SET n = $properties"
            )
            tx.run(q.query(), properties=node.properties)

    def create(self, driver: Driver, database: str = None, batch_size=None):
        """
        Create all nodes in the set.
        """
        if not batch_size:
            batch_size = 1000

        with driver.session(database=database) as session:
            for chunk in chunks(self.nodes, batch_size):
                session.execute_write(self.create_nodes, chunk)

    def merge_nodes(self, tx, nodes: List[Node]):
        for node in nodes:
            q = CypherQuery(
                merge_clause_with_properties(node.labels, node.merge_keys, prop_name="$properties", node_variable="n"),
                "SET n = $properties"
            )

            tx.run(q.query(), properties=node.properties)

    def merge(self, driver: Driver, database: str = None, batch_size=None):
        """
        Merge all nodes in the set.
        """
        if not batch_size:
            batch_size = 1000

        with driver.session(database=database) as session:
            for chunk in chunks(self.nodes, batch_size):
                session.execute_write(self.merge_nodes, chunk)
