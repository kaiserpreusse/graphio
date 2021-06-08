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

    def test_merge_properties_type_error(self):
        class TestNode(ModelNode):
            test = Label('Test')
            name = MergeKey('name')

        with raises(TypeError):
            t = TestNode(foo='Peter')

    def test_exists(self, graph, clear_graph):
        graph.run("CREATE (t:Test) SET t.name = 'Peter'")

        class TestNode(ModelNode):
            test = Label('Test')
            name = MergeKey('name')

        t = TestNode(name='Peter')
        assert t.exists(graph)

        not_t = TestNode(name='NotPeter')
        assert not_t.exists(graph) == False

    def test_exists_raises_type_error(self, graph, clear_graph):
        # create some more nodes
        graph.run("UNWIND [1, 2] AS i CREATE (t:Test) SET t.name = 'Peter'")

        class TestNode(ModelNode):
            test = Label('Test')
            name = MergeKey('name')

        t = TestNode(name='Peter')
        with raises(TypeError):
            t.exists(graph)

    def test_simple_merge(self, graph, clear_graph):
        class TestNode(ModelNode):
            test = Label('Test')
            name = MergeKey('name')

        some_test = TestNode(name='Peter')
        some_test.merge(graph)

        result = graph.run("MATCH (t:Test) WHERE t.name = 'Peter' RETURN count(t) as num").data()
        assert result[0]['num'] == 1

        # try again, should not overwrite
        some_test.merge(graph)
        result = graph.run("MATCH (t:Test) WHERE t.name = 'Peter' RETURN count(t) as num").data()
        assert result[0]['num'] == 1

    def test_merge_additional_properties(self, graph, clear_graph):
        class TestNode(ModelNode):
            test = Label('Test')
            name = MergeKey('name')

        some_test = TestNode(name='Peter', city='London')
        some_test.merge(graph)

        result = graph.run("MATCH (t:Test) WHERE t.name = 'Peter' and t.city = 'London' RETURN count(t) as num").data()
        assert result[0]['num'] == 1

        # try again, should not overwrite
        some_test.merge(graph)
        result = graph.run("MATCH (t:Test) WHERE t.name = 'Peter' and t.city = 'London' RETURN count(t) as num").data()
        assert result[0]['num'] == 1

    def test_error_if_not_unique(self, graph, clear_graph):
        # create some nodes
        graph.run("UNWIND [1, 2] AS i CREATE (t:Test) SET t.name = 'Peter'")

        class TestNode(ModelNode):
            test = Label('Test')
            name = MergeKey('name')

        some_test = TestNode(name='Peter')

        with raises(TypeError):
            some_test.merge(graph)

    def test_link_object_instances(self, graph, clear_graph):
        class TestNode(ModelNode):
            test = Label('Test')
            name = MergeKey('name')

        class Friend(ModelRelationship):
            source = TestNode
            target = TestNode
            type = 'FRIEND'

        peter = TestNode(name='Peter')
        pan = TestNode(name='Pan')

        peter.merge(graph)
        pan.merge(graph)

        peter.link(graph, Friend, pan)

        result = graph.run(
            "MATCH (:Test {name: 'Peter'})-[r:FRIEND]->(:Test {name: 'Pan'}) RETURN count(r) as num").data()

        assert result[0]['num'] > 0

    def test_link_object_decription(self, graph, clear_graph):
        class TestNode(ModelNode):
            test = Label('Test')
            name = MergeKey('name')

        peter = TestNode(name='Peter')
        pan = TestNode(name='Pan')

        peter.merge(graph)
        pan.merge(graph)

        peter.link(graph, 'FRIEND', NodeDescriptor(['Test'], {'name': 'Pan'}))

        result = graph.run(
            "MATCH (:Test {name: 'Peter'})-[r:FRIEND]->(:Test {name: 'Pan'}) RETURN count(r) as num").data()

        assert result[0]['num'] > 0



class TestModelRelationshipInstance:
    """
    Test functionality of ModelRelationship instances.
    """

    def test_exists(self, graph, clear_graph):
        class TestNode(ModelNode):
            test = Label('Test')
            name = MergeKey('name')

        class Friend(ModelRelationship):
            source = TestNode
            target = TestNode
            type = 'FRIEND'

        peter = TestNode(name='Peter')
        pan = TestNode(name='Pan')

        peter_friend_pan = Friend(peter, pan)

        # assert rel does not exist
        assert not peter_friend_pan.exists(graph)

        # create nodes, assert still does not exist
        graph.run("CREATE (:Test {name: 'Peter'}), (:Test {name: 'Pan'})")
        assert not peter_friend_pan.exists(graph)

        # create relationship
        graph.run("MATCH (s:Test {name: 'Peter'}), (t:Test {name: 'Pan'}) CREATE (s)-[:FRIEND]->(t)")

        assert peter_friend_pan.exists(graph)


def test_node_creation(graph, clear_graph):
    class TestNode(ModelNode):
        test = Label('Test')
        name = MergeKey('name')

    ns = TestNode.dataset()

    for i in range(100):
        ns.add_node({'name': i})

    ns.create(graph)

    result = graph.run("MATCH (t:Test) RETURN count(t) AS num").data()

    assert result[0]['num'] == 100

    ns.create(graph)

    result = graph.run("MATCH (t:Test) RETURN count(t) AS num").data()

    assert result[0]['num'] == 200


def test_node_creation_empty(graph, clear_graph):
    class TestNode(ModelNode):
        Test = Label()
        name = MergeKey()

    ns = TestNode.dataset()

    for i in range(100):
        ns.add_node({'name': i})

    ns.create(graph)

    result = graph.run("MATCH (t:Test) RETURN count(t) AS num").data()

    assert result[0]['num'] == 100

    ns.create(graph)

    result = graph.run("MATCH (t:Test) RETURN count(t) AS num").data()

    assert result[0]['num'] == 200


def test_create_nodes_using_types_different_values(graph, clear_graph):
    class Test(ModelNode):
        test = Label('Foo')
        name = MergeKey('bar')

    tests = Test.dataset()

    for i in range(100):
        tests.add_node({Test.name: i})

    tests.create(graph)

    result = graph.run("MATCH (t:Foo) RETURN count(t) AS num").data()

    assert result[0]['num'] == 100


def test_relationship_creation(graph, clear_graph):
    class Test(ModelNode):
        test = Label('Test')
        name = MergeKey('name')

    class Target(ModelNode):
        test = Label('Target')
        name = MergeKey('name')

    class TestToTarget(ModelRelationship):
        source = Test
        target = Target
        type = 'MAPS'

    tests = Test.dataset()
    target = Target.dataset()
    rels = TestToTarget.dataset()

    for i in range(100):
        tests.add_node({'name': i})
        target.add_node({'name': i})
        rels.add_relationship({'name': i}, {'name': i}, {'some': 'value'})

    tests.create(graph)
    target.create(graph)
    rels.create(graph)

    result = graph.run(
        "MATCH (t:Test)-[r:MAPS]->(target:Target) RETURN count(distinct t) as test_nodes, count(distinct r) as rels, count(distinct target) as target_nodes").data()
    assert result[0]['test_nodes'] == 100
    assert result[0]['rels'] == 100
    assert result[0]['target_nodes'] == 100


def test_create_using_types(graph, clear_graph):
    class Test(ModelNode):
        test = Label('Test')
        name = MergeKey('name')

    class Target(ModelNode):
        test = Label('Target')
        name = MergeKey('name')

    class TestToTarget(ModelRelationship):
        source = Test
        target = Target
        type = 'MAPS'

    tests = Test.dataset()
    target = Target.dataset()
    rels = TestToTarget.dataset()

    for i in range(100):
        tests.add_node({Test.name: i})
        target.add_node({Target.name: i})
        rels.add_relationship({Test.name: i}, {Target.name: i}, {'some': 'value'})

    tests.create(graph)
    target.create(graph)
    rels.create(graph)

    result = graph.run(
        "MATCH (t:Test)-[r:MAPS]->(target:Target) RETURN count(distinct t) as test_nodes, count(distinct r) as rels, count(distinct target) as target_nodes").data()
    assert result[0]['test_nodes'] == 100
    assert result[0]['rels'] == 100
    assert result[0]['target_nodes'] == 100


def test_create_using_types_with_different_values(graph, clear_graph):
    class Test(ModelNode):
        test = Label('Foo')
        name = MergeKey('bar')

    class Target(ModelNode):
        test = Label('TargetFoo')
        name = MergeKey('targetbar')

    class TestToTarget(ModelRelationship):
        source = Test
        target = Target
        type = 'MAPS'

    tests = Test.dataset()
    target = Target.dataset()
    rels = TestToTarget.dataset()

    for i in range(100):
        tests.add_node({Test.name: i})
        target.add_node({Target.name: i})
        rels.add_relationship({Test.name: i}, {Target.name: i}, {'some': 'value'})

    print(tests)
    print(target)
    print(rels)
    for r in rels.relationships[0:3]:
        print(r)

    tests.create(graph)
    target.create(graph)
    rels.create(graph)

    result = graph.run("MATCH ()-[t:MAPS]->() RETURN count(t) AS num").data()

    assert result[0]['num'] == 100

    result = graph.run(
        "MATCH (t:Foo)-[r:MAPS]->(target:TargetFoo) RETURN count(distinct t) as test_nodes, count(distinct r) as rels, count(distinct target) as target_nodes").data()
    assert result[0]['test_nodes'] == 100
    assert result[0]['rels'] == 100
    assert result[0]['target_nodes'] == 100
