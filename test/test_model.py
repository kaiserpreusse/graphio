from graphio.model import ModelNode, ModelRelationship, Label, MergeKey

class TestModelNode:

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

    result = graph.run("MATCH (t:Test)-[r:MAPS]->(target:Target) RETURN count(distinct t) as test_nodes, count(distinct r) as rels, count(distinct target) as target_nodes").data()
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

    result = graph.run("MATCH (t:Test)-[r:MAPS]->(target:Target) RETURN count(distinct t) as test_nodes, count(distinct r) as rels, count(distinct target) as target_nodes").data()
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

    # result = graph.run("MATCH (t:Foo)-[r:MAPS]->(target:TargetFoo) RETURN count(distinct t) as test_nodes, count(distinct r) as rels, count(distinct target) as target_nodes").data()
    # assert result[0]['test_nodes'] == 100
    # assert result[0]['rels'] == 100
    # assert result[0]['target_nodes'] == 100
