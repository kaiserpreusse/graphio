from uuid import UUID, uuid4
from datetime import datetime
from typing import List
from neo4j import Driver, DEFAULT_DATABASE

from pydantic import BaseModel, Field

from graphio import NodeSet, RelationshipSet, NodeSetDefinition, RelationshipSetDefinition


class GraphUpdate(BaseModel):
    uuid: str = Field(default_factory=lambda: str(uuid4()))
    created: datetime = Field(default_factory=datetime.utcnow)

    nodesets: List[NodeSetDefinition] = []
    relationshipsets: List[RelationshipSetDefinition] = []

    start_time: datetime = None
    finish_time: datetime = None

    driver: Driver = None
    database: str = DEFAULT_DATABASE

    class Config:
        arbitrary_types_allowed = True

    def props(self):
        """Return node properties for GraphUpdate"""
        return {
            'uuid': self.uuid,
            'created': self.created,
            'start_time': self.start_time,
        }

    def start(self, driver: Driver, database: str = None):
        self.driver = driver
        self.start_time = datetime.utcnow()
        if database:
            self.database = database
            
        with self.driver.session(database=self.database) as session:
            q = """MERGE (u:GraphUpdate {uuid: $uuid}) SET u += $properties"""
            session.run(q, uuid=self.uuid, properties=self.props())

    def add_nodeset(self, nodeset: NodeSetDefinition):
        """Add a NodeSet that was created outside of the GraphUpdate object."""
        self.nodesets.append(nodeset)

        with self.driver.session(database=self.database) as session:
            # merge the nodeset
            q = "MERGE (ns:NodeSet {uuid: $uuid}) SET ns += $properties"
            session.run(q, uuid=nodeset.uuid, properties=nodeset.props())

            # create the relationship between the nodeset and the graphupdate
            q = "MATCH (ns:NodeSet {uuid: $nodeset_uuid}), (gu:GraphUpdate {uuid: $graphupdate_uuid}) MERGE (gu)-[:CONTAINS]->(ns)"
            session.run(q, nodeset_uuid=nodeset.uuid, graphupdate_uuid=self.uuid)

    def add_relationshipset(self, relationshipset: RelationshipSetDefinition):
        """Add a RelationshipSet that was created outside of the GraphUpdate object."""
        self.relationshipsets.append(relationshipset)

        with self.driver.session(database=self.database) as session:
            # merge the relationshipset
            q = "MERGE (rs:RelationshipSet {uuid: $uuid}) SET rs += $properties"
            session.run(q, uuid=relationshipset.uuid, properties=relationshipset.props())

            # create the relationship between the relationshipset and the graphupdate
            q = "MATCH (rs:RelationshipSet {uuid: $relationshipset_uuid}), (gu:GraphUpdate {uuid: $graphupdate_uuid}) MERGE (gu)-[:CONTAINS]->(rs)"
            session.run(q, relationshipset_uuid=relationshipset.uuid, graphupdate_uuid=self.uuid)

    def finish(self):
        self.finish_time = datetime.utcnow()
        with self.driver.session(database=self.database) as session:
            q = "MATCH (gu:GraphUpdate {uuid: $uuid}) SET gu.finish_time = $time"
            session.run(q, time=self.finish_time, uuid=self.uuid)
