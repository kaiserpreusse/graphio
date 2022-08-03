from pytest import raises

from graphio.model import ModelNode, ModelRelationship, Label, MergeKey, NodeDescriptor


class TestNodeDescriptor:

    def test_node_descriptor_constructor(self):
        nd = NodeDescriptor(labels=['Person'], properties={'name': 'Peter'}, merge_keys=['name'])
        assert nd.labels == ['Person']
        assert nd.properties == {'name': 'Peter'}
        assert nd.merge_keys == ['name']

    def test_node_descriptor_constructor_no_merge_keys(self):
        nd = NodeDescriptor(labels=['Person'], properties={'name': 'Peter'})
        assert nd.labels == ['Person']
        assert nd.properties == {'name': 'Peter'}
        assert nd.merge_keys == ['name']


class TestModelNodeClass:
    """
    Test functions on class level.
    """

    def test_instance(self):
        class Test(ModelNode):
            test = Label('test')
            sid = MergeKey('sid')
            foo = MergeKey('foo')

        assert Test.sid == 'sid'
        assert Test.foo == 'foo'

    def test_attribute_access(self):
        class Test(ModelNode):
            test = Label('test')
            sid = MergeKey('sid')

        assert isinstance(Test.test, str)
        assert isinstance(Test.sid, str)

    def test_attribute_different_value(self):
        class Test(ModelNode):
            test = Label('foo')
            sid = MergeKey('bar')

        assert Test.test == 'foo'
        assert Test.sid == 'bar'

    def test_empty_attributes(self):
        class Test(ModelNode):
            Test = Label()
            sid = MergeKey()

        assert Test.Test == 'Test'
        assert Test.sid == 'sid'

    def test_empty_attributes_no_label(self):
        class Test(ModelNode):
            sid = MergeKey()

        assert Test.Test == 'Test'
        assert 'Test' in Test.__labels__
        assert Test.sid == 'sid'

    def test_model_node_factory(self):
        SomeNodeClass = ModelNode.factory(['Person'], merge_keys=['name'], name='PersonClass')
        assert issubclass(SomeNodeClass, ModelNode)
        assert SomeNodeClass.__name__ == 'PersonClass'
        assert SomeNodeClass.__labels__ == ['Person']
        assert SomeNodeClass.__merge_keys__ == ['name']


class TestModelNodeInstance:
    """
    Test functionalities to create instances of model nodes.
    """

    def test_merge_properties(self):
        class TestNode(ModelNode):
            test = Label('Test')
            name = MergeKey('name')

        t = TestNode(name='Peter')
        assert t.merge_props == {'name': 'Peter'}
