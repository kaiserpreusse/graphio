from graphio.utils import create_single_index, create_composite_index, run_query_return_results, convert_neo4j_types_to_python
from neo4j.time import DateTime as Neo4jDateTime
from neo4j.time import Date as Neo4jDate
import datetime


def test_create_query_fixed_property(graph, clear_graph):
    q = "CREATE (a:Test) SET a.key = 'value'"

    run_query_return_results(graph, q)

    r = run_query_return_results(graph, "MATCH (a:Test) RETURN count(a)")
    assert r[0][0] == 1


def test_create_single_index(graph, clear_graph):
    test_label = 'Foo'
    test_prop = 'bar'

    create_single_index(graph, test_label, test_prop)

    # TODO keep until 4.2 is not supported anymore
    try:
        result = run_query_return_results(graph, "SHOW INDEXES YIELD *")
    except:
        result = run_query_return_results(graph, "CALL db.indexes()")
    row = result[0]

    # the result of the db.indexes() procedure is different for Neo4j 3.5 and 4
    # this should also be synced with differences in py2neo versions
    if 'tokenNames' in row:
        assert row['tokenNames'] == [test_label]
        assert row['properties'] == [test_prop]

    elif 'labelsOrTypes' in row:
        assert row['labelsOrTypes'] == [test_label]
        assert row['properties'] == [test_prop]


def test_create_composite_index(graph, clear_graph):
    test_label = 'Foo'
    test_properties = ['bar', 'keks']

    create_composite_index(graph, test_label, test_properties)

    # TODO keep until 4.2 is not supported anymore
    try:
        result = run_query_return_results(graph, "SHOW INDEXES YIELD *")
    except:
        result = run_query_return_results(graph, "CALL db.indexes()")

    row = result[0]

    # the result of the db.indexes() procedure is different for Neo4j 3.5 and 4
    # this should also be synced with differences in py2neo versions
    if 'tokenNames' in row:
        assert row['tokenNames'] == [test_label]
        # cast to set in case lists have different order
        assert set(row['properties']) == set(test_properties)

    elif 'labelsOrTypes' in row:
        assert row['labelsOrTypes'] == [test_label]
        # cast to set in case lists have different order
        assert set(row['properties']) == set(test_properties)


class TestConvertNeo4jValues:
    def test_convert_datetime(self):
        """Test converting a Neo4jDateTime to Python datetime"""
        neo4j_dt = Neo4jDateTime(2023, 5, 15, 14, 30, 45, 123456000)
        result = convert_neo4j_types_to_python(neo4j_dt)

        assert isinstance(result, datetime.datetime)
        assert result.year == 2023
        assert result.month == 5
        assert result.day == 15
        assert result.hour == 14
        assert result.minute == 30
        assert result.second == 45
        assert result.microsecond == 123456
        assert result.tzinfo is None

    def test_convert_datetime_with_timezone(self):
        """Test converting a Neo4jDateTime with timezone to Python datetime"""
        neo4j_dt = Neo4jDateTime(2023, 5, 15, 14, 30, 45, 123456000, tzinfo=1)
        result = convert_neo4j_types_to_python(neo4j_dt)

        assert isinstance(result, datetime.datetime)
        assert result.tzinfo == datetime.timezone.utc

    def test_convert_dict(self):
        """Test converting a dictionary containing Neo4jDateTime"""
        test_dict = {
            'created': Neo4jDateTime(2023, 5, 15, 14, 30, 45, 0),
            'name': 'test',
            'count': 42
        }

        result = convert_neo4j_types_to_python(test_dict)

        assert isinstance(result['created'], datetime.datetime)
        assert result['name'] == 'test'
        assert result['count'] == 42

    def test_convert_list(self):
        """Test converting a list containing Neo4jDateTime"""
        test_list = [
            Neo4jDateTime(2023, 5, 15, 14, 30, 45, 0),
            'test',
            42
        ]

        result = convert_neo4j_types_to_python(test_list)

        assert isinstance(result[0], datetime.datetime)
        assert result[1] == 'test'
        assert result[2] == 42

    def test_convert_nested_structures(self):
        """Test converting nested structures containing Neo4jDateTime"""
        test_nested = {
            'dates': [
                Neo4jDateTime(2023, 5, 15, 14, 30, 45, 0),
                Neo4jDateTime(2023, 6, 16, 15, 31, 46, 0)
            ],
            'metadata': {
                'created': Neo4jDateTime(2023, 7, 17, 16, 32, 47, 0),
                'count': 42
            }
        }

        result = convert_neo4j_types_to_python(test_nested)

        assert isinstance(result['dates'][0], datetime.datetime)
        assert isinstance(result['dates'][1], datetime.datetime)
        assert isinstance(result['metadata']['created'], datetime.datetime)
        assert result['metadata']['count'] == 42

    def test_pass_through_other_types(self):
        """Test that other data types pass through unchanged"""
        test_data = {
            'string': 'test',
            'integer': 42,
            'float': 3.14,
            'boolean': True,
            'none': None,
            'list': [1, 2, 3],
            'dict': {'a': 1, 'b': 2}
        }

        result = convert_neo4j_types_to_python(test_data)

        assert result == test_data

    def test_none_value(self):
        """Test that None passes through unchanged"""
        assert convert_neo4j_types_to_python(None) is None

    def test_convert_date(self):
        """Test converting a Neo4jDate to Python date"""
        neo4j_date = Neo4jDate(2023, 5, 15)
        result = convert_neo4j_types_to_python(neo4j_date)

        assert isinstance(result, datetime.date)
        assert result.year == 2023
        assert result.month == 5
        assert result.day == 15
        assert not hasattr(result, 'hour')  # Ensure it's a date, not datetime

    def test_convert_dict_with_date(self):
        """Test converting a dictionary containing Neo4jDate"""
        test_dict = {
            'created_date': Neo4jDate(2023, 5, 15),
            'name': 'test',
            'count': 42
        }

        result = convert_neo4j_types_to_python(test_dict)

        assert isinstance(result['created_date'], datetime.date)
        assert result['created_date'].year == 2023
        assert result['created_date'].month == 5
        assert result['created_date'].day == 15
        assert result['name'] == 'test'
        assert result['count'] == 42

    def test_convert_mixed_datetime_and_date(self):
        """Test converting structures with both Neo4jDateTime and Neo4jDate"""
        test_data = {
            'event_date': Neo4jDate(2023, 5, 15),
            'event_timestamp': Neo4jDateTime(2023, 5, 15, 14, 30, 45, 0),
            'items': [
                {'date': Neo4jDate(2023, 6, 1)},
                {'timestamp': Neo4jDateTime(2023, 6, 1, 9, 0, 0, 0)}
            ]
        }

        result = convert_neo4j_types_to_python(test_data)

        # Check date conversion
        assert isinstance(result['event_date'], datetime.date)
        assert not isinstance(result['event_date'], datetime.datetime)
        assert result['event_date'].year == 2023
        assert result['event_date'].month == 5
        assert result['event_date'].day == 15

        # Check datetime conversion
        assert isinstance(result['event_timestamp'], datetime.datetime)
        assert result['event_timestamp'].hour == 14
        assert result['event_timestamp'].minute == 30

        # Check nested conversions
        assert isinstance(result['items'][0]['date'], datetime.date)
        assert isinstance(result['items'][1]['timestamp'], datetime.datetime)