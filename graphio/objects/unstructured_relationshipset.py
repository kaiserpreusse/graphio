from typing import List

from pydantic import BaseModel
from neo4j import Driver, Transaction

from graphio.queries import CypherQuery
from graphio.helper import chunks, create_single_index, create_composite_index
from graphio.queries import merge_clause_with_properties


class NodeMatch(BaseModel):
    """Define how to match a node."""
    labels: List[str]
    properties: dict


class Relationship(BaseModel):
    """Define a relationship."""
    start_node: NodeMatch
    end_node: NodeMatch
    type: str
    properties: dict = {}


class UnstructuredRelationshipSet(BaseModel):
    """
    A set of relationships that do not have the same start/end nodes.
    """
    relationships: List[Relationship] = []

    def add_relationship(self, relationship: Relationship):
        self.relationships.append(relationship)

    @property
    def unique_node_definitions(self):
        """
        Return a unique list of label/merge key combinations used to define start node and end node match.
        """
        unique_nodes = set()

        for relationship in self.relationships:
            start_node_def = (tuple(relationship.start_node.labels), tuple(relationship.start_node.properties.keys()))
            end_node_def = (tuple(relationship.end_node.labels), tuple(relationship.end_node.properties.keys()))
            if start_node_def not in unique_nodes:
                unique_nodes.add(start_node_def)
            if end_node_def not in unique_nodes:
                unique_nodes.add(end_node_def)

        return unique_nodes

    def create_index(self, driver: Driver, database: str = None):
        for labels, merge_keys in self.unique_node_definitions:
            for label in labels:
                for merge_key in merge_keys:
                    create_single_index(driver, label, merge_key, database=database)
                if len(merge_keys) > 1:
                    create_composite_index(driver, label, merge_keys, database=database)

    @staticmethod
    def create_relationships(tx, relationships: List[Relationship]):
        for relationship in relationships:

            q = CypherQuery(
                f"MATCH (a:{':'.join(relationship.start_node.labels)}), (b:{':'.join(relationship.end_node.labels)})"
            )

            # collect WHERE clauses
            where_clauses = []
            for property in relationship.start_node.properties.keys():
                where_clauses.append(f'a.{property} = $start_node_properties.{property}')
            for property in relationship.end_node.properties.keys():
                where_clauses.append(f'b.{property} = $end_node_properties.{property}')

            q.append("WHERE " + ' AND '.join(where_clauses))

            q.append(f"CREATE (a)-[r:{relationship.type}]->(b)")
            q.append("SET r = $relationship_properties RETURN count(r)")

            tx.run(q.query(), start_node_properties=relationship.start_node.properties,
                   end_node_properties=relationship.end_node.properties,
                   relationship_properties=relationship.properties)

    def create(self, driver: Driver, database: str = None, batch_size=None):
        """
        Create all relationships in the set.
        """
        if not batch_size:
            batch_size = 1000

        with driver.session(database=database) as session:
            for chunk in chunks(self.relationships, batch_size):
                session.execute_write(self.create_relationships, chunk)

    @staticmethod
    def merge_relationships(tx, relationships: List[Relationship]):
        for relationship in relationships:

            q = CypherQuery(
                f"MATCH (a:{':'.join(relationship.start_node.labels)}), (b:{':'.join(relationship.end_node.labels)})"
            )

            # collect WHERE clauses
            where_clauses = []
            for property in relationship.start_node.properties.keys():
                where_clauses.append(f'a.{property} = $start_node_properties.{property}')
            for property in relationship.end_node.properties.keys():
                where_clauses.append(f'b.{property} = $end_node_properties.{property}')

            q.append("WHERE " + ' AND '.join(where_clauses))

            q.append(f"MERGE (a)-[r:{relationship.type}]->(b)")
            q.append("SET r = $relationship_properties RETURN count(r)")

            tx.run(q.query(), start_node_properties=relationship.start_node.properties,
                   end_node_properties=relationship.end_node.properties,
                   relationship_properties=relationship.properties)

    def merge(self, driver: Driver, database: str = None, batch_size=None):
        """
        Merge all relationships in the set.
        """
        if not batch_size:
            batch_size = 1000

        with driver.session(database=database) as session:
            for chunk in chunks(self.relationships, batch_size):
                session.execute_write(self.merge_relationships, chunk)