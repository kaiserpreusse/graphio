import pytest
from typing import List

from graphio import NodeModel, RelationshipModel, GraphModel, Relationship


class TestGraphModel:

    def test_graph_model(self):
        graph_model = GraphModel()
        assert isinstance(graph_model, GraphModel)

        class MyNode(NodeModel):
            labels = ['Person']
            merge_keys = ['name']

        class MyRelationship(RelationshipModel):
            rel_type: str = 'KNOWS'
            source = MyNode
            target = MyNode

        graph_model.add(MyNode)
        graph_model.add(MyRelationship)
        print(graph_model.nodes)

        assert len(graph_model.nodes) == 1
        assert len(graph_model.relationships) == 1
        assert issubclass(graph_model.nodes[0], NodeModel)
        assert issubclass(graph_model.relationships[0], RelationshipModel)


class TestRegistryMeta:
    def test_registry_meta(self):
        class MyNode(NodeModel):
            labels = ['Person']
            merge_keys = ['name']

        assert type(NodeModel.get_class_by_name('MyNode')) == type(MyNode)


class TestNodeModel:

    def test_create_node_model(self):
        class MyNode(NodeModel):
            labels: List[str] = ['Person']
            merge_keys: List[str] = ['name']

        node_model = MyNode()
        assert isinstance(node_model, NodeModel)
        assert node_model.labels == ['Person']
        assert node_model.merge_keys == ['name']

    def test_get_nodeset(self):
        class MyNode(NodeModel):
            labels: List[str] = ['Person']
            merge_keys: List[str] = ['name']

        node_set = MyNode.nodeset()

        assert node_set.labels == ['Person']
        assert node_set.merge_keys == ['name']

    def test_relationship_to(self):
        # reset the registry
        NodeModel.registry = []
        class MyNode(NodeModel):
            labels = ['Person']
            merge_keys = ['name']

            friends = Relationship('MyNode', 'FRIENDS', 'MyNode')

        relset = MyNode.friends.dataset()

        assert relset.rel_type == 'FRIENDS'
        assert relset.start_node_labels == ['Person']
        assert relset.end_node_labels == ['Person']
        assert relset.start_node_properties == ['name']
        assert relset.end_node_properties == ['name']


class TestRelationshipModel:

    def test_get_relationshiptset(self):
        class MyNode(NodeModel):
            labels: List[str] = ['Person']
            merge_keys: List[str] = ['name']

        class MyRelationship(RelationshipModel):
            rel_type: str = 'KNOWS'
            source: type[NodeModel] = MyNode
            target: type[NodeModel] = MyNode
            end_node_properties: List[str] = ['name']

        relationship_set = MyRelationship.dataset()

        assert relationship_set.rel_type == 'KNOWS'
        assert relationship_set.start_node_labels == ['Person']
        assert relationship_set.end_node_labels == ['Person']
        assert relationship_set.start_node_properties == ['name']
        assert relationship_set.end_node_properties == ['name']
        assert relationship_set.default_props == None
