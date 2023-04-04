# note: integration tests for creating relationships needs nodes in the database
# we create the nodes with graphio, this could mean that issues are difficult to resolve
# however, NodeSets are also tested separately
import os
import json
import pytest
from graphio.objects.nodeset import NodeSet
from graphio.objects.relationshipset import RelationshipSet, tuplify_json_list
from graphio.objects.properties import ArrayProperty
from graphio.graph import run_query_return_results


@pytest.fixture
def small_relationshipset():
    rs = RelationshipSet('TEST', ['Test'], ['Foo'], ['uuid'], ['uuid'])

    for i in range(100):
        rs.add_relationship(
            {'uuid': i}, {'uuid': i}, {}
        )

    return rs


@pytest.fixture
def small_relationshipset_no_labels():
    rs = RelationshipSet('TEST', [], [], ['uuid'], ['uuid'])

    for i in range(100):
        rs.add_relationship(
            {'uuid': i}, {'uuid': i}, {}
        )

    return rs


@pytest.fixture
def small_relationshipset_multiple_labels():
    rs = RelationshipSet('TEST', ['Test', 'Other'], ['Foo', 'SomeLabel'], ['uuid'], ['uuid'])

    for i in range(100):
        rs.add_relationship(
            {'uuid': i}, {'uuid': i}, {}
        )

    return rs


@pytest.fixture
def small_relationshipset_multiple_labels_multiple_merge_keys():
    rs = RelationshipSet('TEST', ['Test', 'Other'], ['Foo', 'SomeLabel'], ['uuid', 'numerical'], ['uuid', 'value'])

    for i in range(100):
        rs.add_relationship(
            {'uuid': i, 'numerical': 1}, {'uuid': i, 'value': 'foo'}, {}
        )

    return rs


@pytest.fixture(scope='function')
def create_nodes_test(graph, clear_graph):
    ns1 = NodeSet(['Test'], merge_keys=['uuid'])
    ns2 = NodeSet(['Foo'], merge_keys=['uuid'])
    ns3 = NodeSet(['Bar'], merge_keys=['uuid', 'key'])

    for i in range(100):
        ns1.add_node({'uuid': i, 'array_key': [i, 9999, 99999]})
        ns2.add_node({'uuid': i, 'array_key': [i, 7777, 77777]})
        ns3.add_node({'uuid': i, 'key': i, 'array_key': [i, 6666, 66666]})

    ns1.create(graph)
    ns2.create(graph)
    ns3.create(graph)

    return ns1, ns2, ns3


def test_str():
    rs = RelationshipSet('TEST', ['Source'], ['Target'], ['uid'], ['name'])

    assert str(rs) == "<RelationshipSet (['Source']; ['uid'])-[TEST]->(['Target']; ['name'])>"


def test_relationship_set_from_dict():
    rs = RelationshipSet('TEST', ['Source'], ['Target'], ['uid'], ['name'])
    rs.add_relationship({'uid': 1}, {'name': 'peter'}, {})
    rs.add_relationship({'uid': 2}, {'name': 'tim'}, {})

    rs_dictionary = rs.to_dict()

    rs_copy = RelationshipSet.from_dict(rs_dictionary)
    assert rs_copy.to_dict() == rs_dictionary


def test__tuplify_json_list():
    l = [[0, 1], {}, [0, 'foo']]

    t = tuplify_json_list(l)

    assert t == ((0, 1), {}, (0, 'foo'))


def test_relationshipset_unique():
    rs = RelationshipSet('TEST', ['Source'], ['Target'], ['uid'], ['name'])
    rs.unique = True
    for i in range(10):
        rs.add_relationship({'uid': 1}, {'name': 'peter'}, {'some': 'value', 'user': 'bar'})
    assert len(rs.relationships) == 1


def test_relationshipset_all_property_keys():
    rs = RelationshipSet('TEST', ['Source'], ['Target'], ['uid'], ['name'])

    random_keys = ['name', 'city', 'value', 'key']

    for val in random_keys:
        for i in range(20):
            rel_props = {}
            rel_props[val] = 'peter'

            rs.add_relationship({'uid': 1}, {'name': 'peter'}, rel_props)

    assert rs.all_property_keys() == set(random_keys)


def test_relationshipset_estiamte_types():
    rs = RelationshipSet('TEST', ['Source'], ['Target'], ['uid'], ['name', 'height', 'age'])

    for i in range(20):
        rs.add_relationship({'uid': 1}, {'name': 'peter', 'height': 20.5, 'age': 50},
                            {'country': 'germany', 'weight': 70.4, 'number': 20})

    start_node_property_types, rel_property_types, end_node_property_types = rs._estimate_type_of_property_values()
    assert start_node_property_types['uid'] == int
    assert end_node_property_types['name'] == str
    assert end_node_property_types['height'] == float
    assert end_node_property_types['age'] == int
    assert rel_property_types['country'] == str
    assert rel_property_types['weight'] == float
    assert rel_property_types['number'] == int



class TestRelationshipSetInstance:

    def test_relationshipset_to_definition(self, small_relationshipset):
        rsdef = small_relationshipset.to_definition()
        assert rsdef.uuid == small_relationshipset.uuid
        assert rsdef.rel_type == 'TEST'
        assert rsdef.start_node_labels == ['Test']
        assert rsdef.end_node_labels == ['Foo']
        assert rsdef.start_node_properties == ['uuid']
        assert rsdef.end_node_properties == ['uuid']


class TestDefaultProps:

    def test_default_props(self):
        rs = RelationshipSet('TEST', ['Source'], ['Target'], ['uid'], ['name'], default_props={'user': 'foo'})
        rs.add_relationship({'uid': 1}, {'name': 'peter'}, {'some': 'value'})
        rs.add_relationship({'uid': 2}, {'name': 'tim'}, {'some': 'value'})

        for n in rs.relationships:
            assert n[2]['user'] == 'foo'

    def test_default_props_overwrite_from_node(self):
        rs = RelationshipSet('TEST', ['Source'], ['Target'], ['uid'], ['name'], default_props={'user': 'foo'})
        rs.add_relationship({'uid': 1}, {'name': 'peter'}, {'some': 'value', 'user': 'bar'})
        rs.add_relationship({'uid': 2}, {'name': 'tim'}, {'some': 'value', 'user': 'bar'})

        for r in rs.relationships:
            assert r[2]['user'] == 'bar'


class TestRelationshipSetCreate:

    def test_relationshipset_create_no_labels(self, graph, create_nodes_test, small_relationshipset_no_labels):

        small_relationshipset_no_labels.create(graph)

        # check if 100 relationships are created for two labels
        result = run_query_return_results(graph, "MATCH (t:Test)-[r:TEST]->(f:Foo) RETURN count(r)")

        assert result[0][0] == 100

        # Test that 900 (3*3*100) relationships are created in total
        result = run_query_return_results(graph, "MATCH (t)-[r:TEST]->(f) RETURN count(r)")

        assert result[0][0] == 900

    def test_relationshipset_create_no_properties(self, graph, create_nodes_test):

        rs = RelationshipSet('TEST', ['Test'], ['Foo'], ['uuid'], ['uuid'])

        for i in range(100):
            rs.add_relationship({'uuid': i}, {'uuid': i})

        rs.create(graph)

        result = run_query_return_results(graph, "MATCH (t:Test)-[r:TEST]->(f:Foo) RETURN count(r)")

        assert result[0][0] == 100

    def test_relationshipset_create_number(self, graph, create_nodes_test, small_relationshipset):

        small_relationshipset.create(graph)

        result = run_query_return_results(graph, "MATCH (t:Test)-[r:TEST]->(f:Foo) RETURN count(r)")

        assert result[0][0] == 100

    def test_relationshipset_create_mulitple_node_props(self, graph, create_nodes_test):

        rs = RelationshipSet('TEST', ['Test'], ['Bar'], ['uuid'], ['uuid', 'key'])

        for i in range(100):
            rs.add_relationship(
                {'uuid': i}, {'uuid': i, 'key': i}, {}
            )

        rs.create(graph)

        result = run_query_return_results(graph, "MATCH (:Test)-[r:TEST]->(:Bar) RETURN count(r)")

        assert result[0][0] == 100

    def test_relationshipset_create_array_props(self, graph, create_nodes_test):

        rs = RelationshipSet('TEST_ARRAY', ['Test'], ['Foo'], [ArrayProperty('array_key')], [ArrayProperty('array_key')])

        for i in range(100):
            rs.add_relationship({'array_key': i}, {'array_key': i})

        rs.create(graph)

        result = run_query_return_results(graph, "MATCH (t:Test)-[r:TEST_ARRAY]->(f:Foo) RETURN count(r)")

        assert result[0][0] == 100

    def test_relationshipset_create_string_and_array_props(self, graph, create_nodes_test):

        rs = RelationshipSet('TEST_ARRAY', ['Test'], ['Foo'], [ArrayProperty('array_key')], [ArrayProperty('array_key')])

        for i in range(100):
            rs.add_relationship({'uuid': i, 'array_key': i}, {'uuid': i, 'array_key': i})

        rs.create(graph)

        result = run_query_return_results(graph, "MATCH (t:Test)-[r:TEST_ARRAY]->(f:Foo) RETURN count(r)")

        assert result[0][0] == 100


class TestRelationshipSetIndex:
    def test_relationship_create_single_index(self, graph, clear_graph, small_relationshipset):

        small_relationshipset.create_index(graph)

        # TODO keep until 4.2 is not supported anymore
        try:
            result = run_query_return_results(graph, "SHOW INDEXES YIELD *")
        except:
            result = run_query_return_results(graph, "CALL db.indexes()")

        for row in result:
            # the result of the db.indexes() procedure is different for Neo4j 3.5 and 4
            # this should also be synced with differences in py2neo versions
            if 'tokenNames' in row:
                assert row['tokenNames'] == ['Test'] and row['properties'] == ['uuid'] \
                       or row['tokenNames'] == ['Test'] and row['properties'] == ['uuid']

            elif 'labelsOrTypes' in row:
                assert row['labelsOrTypes'] == ['Test'] and row['properties'] == ['uuid'] \
                       or row['labelsOrTypes'] == ['Test'] and row['properties'] == ['uuid']


class TestRelationshipSetMerge:

    def test_relationshipset_merge_number(self, graph, create_nodes_test, small_relationshipset):
        small_relationshipset.merge(graph)

        result = run_query_return_results(graph, "MATCH (t:Test)-[r:TEST]->(f:Foo) RETURN count(r)")

        assert result[0][0] == 100

        # merge again to check that number stays the same
        small_relationshipset.merge(graph)

        result = run_query_return_results(graph, "MATCH (t:Test)-[r:TEST]->(f:Foo) RETURN count(r)")

        assert result[0][0] == 100

    def test_relationshipset_merge_no_labels(self, graph, create_nodes_test, small_relationshipset_no_labels):

        small_relationshipset_no_labels.merge(graph)

        # check if 100 relationships are created for two labels
        result = run_query_return_results(graph, "MATCH (t:Test)-[r:TEST]->(f:Foo) RETURN count(r)")

        assert result[0][0] == 100

        # Test that 900 (3*3*100) relationships are created in total
        result = run_query_return_results(graph, "MATCH (t)-[r:TEST]->(f) RETURN count(r)")

        assert result[0][0] == 900

        # merge again to check that number stays the same
        small_relationshipset_no_labels.merge(graph)

        # check if 100 relationships are created for two labels
        result = run_query_return_results(graph, "MATCH (t:Test)-[r:TEST]->(f:Foo) RETURN count(r)")

        assert result[0][0] == 100

        # Test that 900 (3*3*100) relationships are created in total
        result = run_query_return_results(graph, "MATCH (t)-[r:TEST]->(f) RETURN count(r)")

        assert result[0][0] == 900


    def test_relationshipset_merge_array_props(self, graph, create_nodes_test):

        rs = RelationshipSet('TEST_ARRAY', ['Test'], ['Foo'], [ArrayProperty('array_key')], [ArrayProperty('array_key')])

        for i in range(100):
            rs.add_relationship({'array_key': i}, {'array_key': i})

        rs.merge(graph)

        result = run_query_return_results(graph, "MATCH (t:Test)-[r:TEST_ARRAY]->(f:Foo) RETURN count(r)")

        assert result[0][0] == 100

        # merge again
        rs.merge(graph)

        result = run_query_return_results(graph, "MATCH (t:Test)-[r:TEST_ARRAY]->(f:Foo) RETURN count(r)")

        assert result[0][0] == 100

    def test_relationshipset_merge_string_and_array_props(self, graph, create_nodes_test):

        rs = RelationshipSet('TEST_ARRAY', ['Test'], ['Foo'], [ArrayProperty('array_key')], [ArrayProperty('array_key')])

        for i in range(100):
            rs.add_relationship({'uuid': i, 'array_key': i}, {'uuid': i, 'array_key': i})

        rs.merge(graph)

        result = run_query_return_results(graph, "MATCH (t:Test)-[r:TEST_ARRAY]->(f:Foo) RETURN count(r)")

        assert result[0][0] == 100

        # run again
        rs.merge(graph)

        result = run_query_return_results(graph, "MATCH (t:Test)-[r:TEST_ARRAY]->(f:Foo) RETURN count(r)")

        assert result[0][0] == 100

class TestRelationshipSetToJSON:

    def test_object_file_name(self, small_relationshipset):
        # set fixed uuid for relationshipset
        uuid = 'f8d1f0af-3eee-48b4-8407-8694ca628fc0'
        small_relationshipset.uuid = uuid

        assert small_relationshipset.object_file_name() == f"relationshipset_Test_TEST_Foo_f8d1f0af-3eee-48b4-8407-8694ca628fc0"
        assert small_relationshipset.object_file_name(
            suffix='.json') == "relationshipset_Test_TEST_Foo_f8d1f0af-3eee-48b4-8407-8694ca628fc0.json"

    def test_serialize(self, small_relationshipset, small_relationshipset_multiple_labels,
                       small_relationshipset_multiple_labels_multiple_merge_keys, tmp_path):
        """
        Test serialization with different test NodeSets.
        """

        for test_rs in [small_relationshipset, small_relationshipset_multiple_labels,
                        small_relationshipset_multiple_labels_multiple_merge_keys]:
            uuid = 'f8d1f0af-3eee-48b4-8407-8694ca628fc0'
            test_rs.uuid = uuid

            test_rs.to_json(str(tmp_path))

            target_file_path = os.path.join(tmp_path, test_rs.object_file_name(suffix='.json'))

            assert os.path.exists(target_file_path)

            with open(target_file_path, 'rt') as f:
                reloaded_relset = RelationshipSet.from_dict(json.load(f))

                assert reloaded_relset.start_node_labels == test_rs.start_node_labels
                assert reloaded_relset.start_node_properties == test_rs.start_node_properties
                assert reloaded_relset.end_node_labels == test_rs.end_node_labels
                assert reloaded_relset.end_node_properties == test_rs.end_node_properties
                assert reloaded_relset.relationships == test_rs.relationships
                assert len(reloaded_relset.relationships) == len(test_rs.relationships)


class TestRelationshipSetToCSV:

    def test_to_csv(self, tmp_path):
        filepath = os.path.join(tmp_path, 'relationshipset.csv')

        rs = RelationshipSet('TEST', ['Test', 'Other'], ['Foo', 'SomeLabel'], ['uuid', 'numerical'], ['uuid', 'value'])

        for i in range(10):
            rs.add_relationship(
                {'uuid': i, 'numerical': 1}, {'uuid': i, 'value': 'foo'}, {'value': i, 'other_value': 'peter'}
            )

        # add a few relationships with different props
        for i in range(10, 20):
            rs.add_relationship(
                {'uuid': i, 'numerical': 1}, {'uuid': i, 'value': 'foo'},
                {'second_value': i, 'other_second_value': 'peter'}
            )

        csv_file = rs.to_csv(filepath)

        expected_num_of_fields = len(rs.all_property_keys()) + len(rs.fixed_order_start_node_properties) + len(
            rs.fixed_order_end_node_properties)

        with open(csv_file) as f:
            lines = f.readlines()
            assert len(lines) - 1 == len(rs.relationships)

            for l in lines:
                l = l.strip()
                assert len(l.split(',')) == expected_num_of_fields

    def test_create_csv_file_header(self, tmp_path):
        filepath = os.path.join(tmp_path, 'relationshipset.csv')

        rs = RelationshipSet('TEST', ['Test', 'Other'], ['Foo', 'SomeLabel'], ['uuid', 'numerical'], ['uuid', 'value'])

        for i in range(10):
            rs.add_relationship(
                {'uuid': i, 'numerical': 1}, {'uuid': i, 'value': 'foo'}, {'value': i, 'other_value': 'peter'}
            )

        # add a few relationships with different props
        for i in range(10, 20):
            rs.add_relationship(
                {'uuid': i, 'numerical': 1}, {'uuid': i, 'value': 'foo'},
                {'second_value': i, 'other_second_value': 'peter'}
            )

        csv_file = rs.to_csv(filepath)

        with open(csv_file) as f:
            lines = f.readlines()
            # note that csv has header
            assert len(lines) - 1 == len(rs.relationships)

            header = lines[0].strip().split(',')
            assert set(header) == set([f"rel_{x}" for x in rs.all_property_keys()]).union(set([f"start_{x}" for x in rs.fixed_order_start_node_properties])).union(set([f"end_{x}" for x in rs.fixed_order_end_node_properties]))

    def test_create_csv_query(self):
        rs = RelationshipSet('TEST', ['Test', 'Other'], ['Foo', 'SomeLabel'], ['uuid', 'numerical'], ['uuid', 'value'])
        rs.uuid = 'peter'

        for i in range(10):
            rs.add_relationship(
                {'uuid': i, 'numerical': 1}, {'uuid': i, 'value': 'foo'}, {'value': i, 'other_value': 'peter'}
            )

        # add a few relationships with different props
        for i in range(10, 20):
            rs.add_relationship(
                {'uuid': i, 'numerical': 1}, {'uuid': i, 'value': 'foo'},
                {'second_value': i, 'other_second_value': 'peter'}
            )

        query = rs.create_csv_query('CREATE')

        assert query == """USING PERIODIC COMMIT 1000 
LOAD CSV WITH HEADERS FROM 'file:///relationshipset_Test_Other_TEST_Foo_SomeLabel_peter.csv' AS line 
MATCH (a:Test:Other), (b:Foo:SomeLabel) 
WHERE a.uuid = toInteger(line.a_uuid) AND a.numerical = toInteger(line.a_numerical) AND b.uuid = toInteger(line.b_uuid) AND b.value = line.b_value 
CREATE (a)-[r:TEST]->(b) 
SET r.other_second_value = line.rel_other_second_value, r.other_value = line.rel_other_value, r.second_value = toInteger(line.rel_second_value), r.value = line.rel_value"""


class TestRelationshipSetCSVandJSON:
    """
    Test functionality around creating/reading CSV files and associated JSON metadata files.
    """
    def test_read_from_files(self, root_dir, clear_graph, graph):

        files_path = os.path.join(root_dir, "test", "files")

        # load nodes first (taken from NodeSet test)
        nodes_json_file_path = os.path.join(files_path, 'nodes_csv_json.json')
        nodes_csv_file_path = os.path.join(files_path, 'nodes_csv_json.csv')

        ns = NodeSet.from_csv_json_set(nodes_csv_file_path, nodes_json_file_path)
        ns.merge(graph)
        assert run_query_return_results(graph, "MATCH (n:Test) RETURN count(n)")[0][0] == 2

        # create relset and load relationships
        json_file_path = os.path.join(files_path, 'rels_csv_json.json')
        csv_file_path = os.path.join(files_path, 'rels_csv_json.csv')

        assert os.path.exists(json_file_path)
        assert os.path.exists(csv_file_path)

        rs = RelationshipSet.from_csv_json_set(csv_file_path, json_file_path)

        assert rs.start_node_labels == ['Test']
        assert rs.end_node_labels == ['Test']
        assert rs.start_node_properties == ['test_id']
        assert rs.end_node_properties == ['test_id']
        assert rs.rel_type == 'RELATIONSHIP'

        rs.merge(graph)

        assert run_query_return_results(graph, "MATCH (:Test)-[r:RELATIONSHIP]->(:Test) RETURN count(r)")[0][0] == 1

    def test_read_from_files_load_items_to_memory(self, root_dir, clear_graph, graph):

        files_path = os.path.join(root_dir, "test", "files")

        # load nodes first (taken from NodeSet test)
        nodes_json_file_path = os.path.join(files_path, 'nodes_csv_json.json')
        nodes_csv_file_path = os.path.join(files_path, 'nodes_csv_json.csv')

        ns = NodeSet.from_csv_json_set(nodes_csv_file_path, nodes_json_file_path, load_items=True)
        ns.merge(graph)
        assert run_query_return_results(graph, "MATCH (n:Test) RETURN count(n)")[0][0] == 2

        # create relset and load relationships
        json_file_path = os.path.join(files_path, 'rels_csv_json.json')
        csv_file_path = os.path.join(files_path, 'rels_csv_json.csv')

        assert os.path.exists(json_file_path)
        assert os.path.exists(csv_file_path)

        rs = RelationshipSet.from_csv_json_set(csv_file_path, json_file_path, load_items=True)

        assert rs.start_node_labels == ['Test']
        assert rs.end_node_labels == ['Test']
        assert rs.start_node_properties == ['test_id']
        assert rs.end_node_properties == ['test_id']
        assert rs.rel_type == 'RELATIONSHIP'

        rs.merge(graph)

        assert run_query_return_results(graph, "MATCH (:Test)-[r:RELATIONSHIP]->(:Test) RETURN count(r)")[0][0] == 1

    def test_write_files_read_again_load_items_into_memory(self, graph, clear_graph, create_nodes_test, tmp_path, small_relationshipset):
        csv_file_path = os.path.join(tmp_path, 'rels.csv')
        json_file_path = os.path.join(tmp_path, 'rels.json')

        small_relationshipset.to_csv_json_set(csv_file_path, json_file_path)

        assert os.path.exists(csv_file_path)
        assert os.path.exists(json_file_path)

        #print(open(csv_file_path).read())
        #print(open(json_file_path).read())

        rs = RelationshipSet.from_csv_json_set(csv_file_path, json_file_path, load_items=True)
        assert rs.start_node_labels == small_relationshipset.start_node_labels
        assert rs.end_node_labels == small_relationshipset.end_node_labels
        assert rs.start_node_properties == small_relationshipset.start_node_properties
        assert rs.end_node_properties == small_relationshipset.end_node_properties
        assert rs.rel_type == small_relationshipset.rel_type
        assert len(rs.relationships) == 100

        result = run_query_return_results(graph, "MATCH (t:Test)-[r:TEST]->(f:Foo) RETURN count(r)")

        #assert result[0][0] == 100