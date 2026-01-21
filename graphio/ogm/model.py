import logging
from typing import Any, ClassVar

from neo4j import DEFAULT_DATABASE, Driver
from pydantic import BaseModel, ConfigDict, PrivateAttr

from graphio.bulk import NodeSet, RelationshipSet
from graphio.ogm.query_utils import where_clause_with_properties
from graphio.utils import convert_neo4j_types_to_python, get_label_string_from_list_of_labels

log = logging.getLogger(__name__)

_MODEL_REGISTRY = {}


class CypherQuery:
    """Helper class for executing raw Cypher queries in NodeModel.match()"""

    def __init__(self, query, params=None, **kwargs):
        self.query = query
        self.params = params or {}
        self.params.update(kwargs)


class FilterOp:
    """
    Filter operation class for complex querying.

    Supported operators: =, <>, >, <, >=, <=, STARTS WITH, ENDS WITH, CONTAINS
    """

    def __init__(self, field, operator, value):
        """
        Create a new filter operation.

        :param field: The field name to filter on
        :param operator: The operator (=, <>, >, <, >=, <=, etc.)
        :param value: The value to compare against
        """
        self.field = field
        self.operator = operator
        self.value = value


class QueryFieldDescriptor:
    """Descriptor that allows class fields to be used directly in match conditions"""

    def __init__(self, name):
        self.name = name

    def __get__(self, instance, owner):
        if instance is None:
            # Class access - return query helper object
            return RelField(self.name)
        # Instance access - pass through to normal attribute access
        return instance.__dict__.get(self.name)


class RelField:
    """Helper class for creating filter operations on both node and relationship properties"""

    def __init__(self, field_name):
        self.field_name = field_name

    def __eq__(self, other):
        return FilterOp(self.field_name, '=', other)

    def __gt__(self, other):
        return FilterOp(self.field_name, '>', other)

    def __lt__(self, other):
        return FilterOp(self.field_name, '<', other)

    def __ge__(self, other):
        return FilterOp(self.field_name, '>=', other)

    def __le__(self, other):
        return FilterOp(self.field_name, '<=', other)

    def __ne__(self, other):
        return FilterOp(self.field_name, '<>', other)

    def starts_with(self, value):
        return FilterOp(self.field_name, 'STARTS WITH', value)

    def ends_with(self, value):
        return FilterOp(self.field_name, 'ENDS WITH', value)

    def contains(self, value):
        return FilterOp(self.field_name, 'CONTAINS', value)


class CustomMeta(BaseModel.__class__):
    def __new__(mcs, name, bases, attrs):
        # First create the class using the normal mechanism
        cls = super().__new__(mcs, name, bases, attrs)

        # Add field descriptors for all fields in model classes
        if name not in ('Base', 'NodeModel'):
            if hasattr(cls, 'model_fields'):
                for field_name in cls.model_fields:
                    # Check if the field's default value is a descriptor (like Relationship)
                    field_info = cls.model_fields[field_name]
                    default_value = field_info.default
                    if default_value is not None and hasattr(default_value, '__get__'):
                        # Restore the descriptor to the class so it works properly
                        setattr(cls, field_name, default_value)
                    else:
                        setattr(cls, field_name, QueryFieldDescriptor(field_name))

        # Simple import-time registration - replaces complex Registry
        if name not in ('Base', 'NodeModel'):
            if hasattr(cls, '_labels') and hasattr(cls, '_merge_keys'):
                _MODEL_REGISTRY[cls.__name__] = cls

        return cls


class PathStep:
    """Represents one step in a graph traversal path"""

    def __init__(self, node_class, rel_type=None, node_filters=None, rel_filters=None):
        self.node_class = node_class
        self.rel_type = rel_type  # None for starting node
        self.node_filters = node_filters or []
        self.rel_filters = rel_filters or []


class RelationshipTraversal:
    """Helper class for relationship traversal that provides a clean interface for class-level queries"""

    def __init__(self, query):
        self.query = query

    def filter(self, *filter_ops):
        """Add relationship filters - context makes this apply to relationship properties"""
        return RelationshipTraversal(self.query.filter(*filter_ops, _context='relationship'))

    def match(self, *filter_ops):
        """Add node filters to target nodes"""
        return self.query.filter(*filter_ops)

    def all(self):
        """Execute query and return all results"""
        return self.query.all()

    def first(self):
        """Execute query and return first result"""
        return self.query.first()


class Query:
    """Unified query builder for graph traversal patterns (0-hop and 1-hop)"""

    def __init__(self, start_class, source_instance=None, path=None, database=None):
        self.start_class = start_class
        self.source_instance = (
            source_instance  # For instance-based queries like alice.knows.match()
        )
        self.path = path or [PathStep(start_class)]  # Always start with the initial node
        self.database = database

    @property
    def current_step(self):
        """Get the current step we're building filters for"""
        return self.path[-1]

    @property
    def target_class(self):
        """Get the class we'll return instances of"""
        return self.current_step.node_class

    def __getattr__(self, name):
        """Handle relationship traversal like Person.match().knows"""
        # Only allow traversal from the start node (0-hop to 1-hop)
        if len(self.path) > 1:
            raise AttributeError('Multi-hop traversal not yet implemented')

        start_step = self.path[0]
        if (
            hasattr(start_step.node_class, '_relationships')
            and name in start_step.node_class._relationships
        ):
            relationship = start_step.node_class._relationships[name]
            from graphio.ogm.model import Base

            # Detect if this is a reverse relationship (querying from target to source)
            # For self-referencing relationships (source == target), always use normal direction
            start_node_name = start_step.node_class.__name__
            is_reverse = (
                start_node_name == relationship.target
                and relationship.source != relationship.target
            )

            if is_reverse:
                # Reverse relationship: querying from target back to source
                target_class = Base.get_class_by_name(relationship.source)
                if not target_class:
                    raise ValueError(f'Could not find source class {relationship.source}')
            else:
                # Normal relationship: querying from source to target
                target_class = Base.get_class_by_name(relationship.target)
                if not target_class:
                    raise ValueError(f'Could not find target class {relationship.target}')

            # Create new Query with 1-hop path, storing the reverse flag in rel_type as a tuple
            new_path = [
                start_step,  # Keep the start step with its filters
                PathStep(target_class, rel_type=(relationship.rel_type, is_reverse)),
            ]

            # Return a RelationshipTraversal helper that supports both filter() and rel_filter()
            return RelationshipTraversal(Query(self.start_class, self.source_instance, new_path, self.database))

        raise AttributeError(f"'{start_step.node_class.__name__}' has no relationship '{name}'")

    def filter(self, *filter_ops, _context='node'):
        """Add filters to current step - context determines if they're node or relationship filters"""
        new_path = list(self.path)  # Copy path
        new_step = PathStep(
            self.current_step.node_class,
            self.current_step.rel_type,
            list(self.current_step.node_filters),  # Copy existing node filters
            list(self.current_step.rel_filters),  # Copy existing rel filters
        )

        # Add filters based on context parameter
        for f in filter_ops:
            if isinstance(f, FilterOp):
                if _context == 'relationship':
                    # Add to relationship filters
                    new_step.rel_filters.append(f)
                else:
                    # Add to node filters (default)
                    new_step.node_filters.append(f)
            elif isinstance(f, CypherQuery):
                # CypherQuery always applies to nodes
                new_step.node_filters.append(f)
            else:
                raise ValueError(f'Unsupported filter type: {type(f)}')

        new_path[-1] = new_step
        return Query(self.start_class, self.source_instance, new_path, self.database)

    def match(self, *filter_ops):
        """Alias for filter() to maintain compatibility"""
        return self.filter(*filter_ops)

    def all(self):
        """Execute the query and return all results"""
        if self.start_class._driver is None:
            raise ValueError('Driver is not set. Use set_driver() to set the driver.')

        # Handle CypherQuery in filters
        for step in self.path:
            for f in step.node_filters:
                if isinstance(f, CypherQuery):
                    return self._execute_cypher_query(f)

        if len(self.path) == 1:
            return self._execute_node_query()
        elif len(self.path) == 2:
            return self._execute_relationship_query()
        else:
            raise NotImplementedError('Multi-hop queries not yet implemented')

    def first(self):
        """Execute the query and return first result"""
        if self.start_class._driver is None:
            raise ValueError('Driver is not set. Use set_driver() to set the driver.')

        # Handle CypherQuery in filters
        for step in self.path:
            for f in step.node_filters:
                if isinstance(f, CypherQuery):
                    log.warning('Using CypherQuery in first() method needs a LIMIT 1 in the query.')
                    results = self._execute_cypher_query(f)
                    return results[0] if results else None

        if len(self.path) == 1:
            return self._execute_node_query(limit=1)
        elif len(self.path) == 2:
            return self._execute_relationship_query(limit=1)
        else:
            raise NotImplementedError('Multi-hop queries not yet implemented')

    def _execute_node_query(self, limit=None):
        """Execute 0-hop node query"""
        step = self.path[0]

        # If we have a source instance, match that specific instance
        if self.source_instance:
            return [self.source_instance] if limit != 1 else self.source_instance

        query = f'MATCH (n{get_label_string_from_list_of_labels(step.node_class._labels)})'
        conditions = []
        params = {}

        # Add node filters
        for i, f in enumerate(step.node_filters):
            param_name = f'n_{f.field}_{i}'
            conditions.append(f'n.{f.field} {f.operator} ${param_name}')
            params[param_name] = f.value

        if conditions:
            query += f'\nWHERE {" AND ".join(conditions)}'

        query += '\nRETURN n'

        if limit is not None:
            query += f' LIMIT {limit}'

        log.debug(query)

        # Execute query
        db = self.database if self.database else self.start_class.get_database()
        results = []
        with step.node_class._driver.session(database=db) as session:
            result = session.run(query, params)
            for record in result:
                node = record['n']
                properties = dict(node.items())
                properties = convert_neo4j_types_to_python(properties)
                results.append(step.node_class(**properties))

        return results[0] if limit == 1 and results else (results if limit != 1 else None)

    def _execute_relationship_query(self, limit=None):
        """Execute 1-hop relationship query"""
        start_step = self.path[0]
        target_step = self.path[1]

        # Extract relationship type and direction
        if isinstance(target_step.rel_type, tuple):
            rel_type, is_reverse = target_step.rel_type
        else:
            rel_type, is_reverse = target_step.rel_type, False

        # Build relationship pattern based on direction
        if is_reverse:
            # Reverse: (source)<-[r:REL]-(target)
            rel_pattern = f'(source{get_label_string_from_list_of_labels(start_step.node_class._labels)})<-[r:{rel_type}]-(target{get_label_string_from_list_of_labels(target_step.node_class._labels)})'
        else:
            # Normal: (source)-[r:REL]->(target)
            rel_pattern = f'(source{get_label_string_from_list_of_labels(start_step.node_class._labels)})-[r:{rel_type}]->(target{get_label_string_from_list_of_labels(target_step.node_class._labels)})'

        # Build Cypher query
        if self.source_instance:
            # Instance-based query
            query = f"""WITH $properties AS properties
MATCH {rel_pattern}
WHERE {where_clause_with_properties(self.source_instance.match_dict, 'properties', node_variable='source')}"""
            params = {'properties': self.source_instance.match_dict}
        else:
            # Class-based query
            query = f'MATCH {rel_pattern}'
            params = {}

            # Add source node filters
            conditions = []
            for i, f in enumerate(start_step.node_filters):
                param_name = f'source_{f.field}_{i}'
                conditions.append(f'source.{f.field} {f.operator} ${param_name}')
                params[param_name] = f.value

            if conditions:
                query += f'\nWHERE {" AND ".join(conditions)}'

        # Add relationship filters
        rel_conditions = []
        for i, f in enumerate(target_step.rel_filters):
            param_name = f'rel_{f.field}_{i}'
            rel_conditions.append(f'r.{f.field} {f.operator} ${param_name}')
            params[param_name] = f.value

        # Add target node filters
        target_conditions = []
        for i, f in enumerate(target_step.node_filters):
            param_name = f'target_{f.field}_{i}'
            target_conditions.append(f'target.{f.field} {f.operator} ${param_name}')
            params[param_name] = f.value

        # Combine additional conditions
        additional_conditions = rel_conditions + target_conditions
        if additional_conditions:
            if 'WHERE' in query:
                query += f'\nAND {" AND ".join(additional_conditions)}'
            else:
                query += f'\nWHERE {" AND ".join(additional_conditions)}'

        query += '\nRETURN DISTINCT target'

        if limit is not None:
            query += f' LIMIT {limit}'

        # Execute query
        db = self.database if self.database else self.start_class.get_database()
        results = []
        with target_step.node_class._driver.session(database=db) as session:
            result = session.run(query, params)
            for record in result:
                node = record['target']
                properties = dict(node.items())
                properties = convert_neo4j_types_to_python(properties)
                results.append(target_step.node_class(**properties))

        return results[0] if limit == 1 and results else (results if limit != 1 else None)

    def _execute_cypher_query(self, cypher_query):
        """Execute a CypherQuery and return results"""
        db = self.database if self.database else self.start_class.get_database()
        nodes = []
        with self.start_class._driver.session(database=db) as session:
            result = session.run(cypher_query.query, cypher_query.params)
            for record in result:
                if 'n' not in record.keys():
                    raise ValueError(
                        f"Query must return nodes with variable name 'n'. Got keys: {record.keys()}"
                    )

                node = record['n']
                properties = dict(node.items())
                properties = convert_neo4j_types_to_python(properties)
                nodes.append(self.target_class(**properties))

        return nodes


class Base(BaseModel, metaclass=CustomMeta):
    """Static base class for all Neo4j ORM models"""

    _driver: ClassVar[Driver | None] = None
    _database: ClassVar[str | None] = None

    @classmethod
    def discover_models(cls):
        """Models are automatically discovered via import-time registration"""
        # No-op: models are automatically registered when imported
        return cls

    @classmethod
    def get_registry(cls):
        """Return the simple model registry dict"""
        return _MODEL_REGISTRY

    @classmethod
    def set_driver(cls, driver: Driver):
        cls._driver = driver
        return cls

    @classmethod
    def get_driver(cls) -> Driver:
        if cls._driver is None:
            raise ValueError('Driver is not set. Use set_driver() to set the driver.')
        return cls._driver

    @classmethod
    def set_database(cls, database: str):
        """Set the default database for all OGM operations"""
        cls._database = database
        return cls

    @classmethod
    def get_database(cls):
        """Get the configured database or DEFAULT_DATABASE"""
        return cls._database if cls._database else DEFAULT_DATABASE

    @classmethod
    def get_class_by_name(cls, name):
        """Get a model class by its name"""
        return _MODEL_REGISTRY.get(name)

    @classmethod
    def model_create_index(cls, database=None):
        """Create indexes for all models in the specified database"""
        db = database if database else Base.get_database()
        for model in _MODEL_REGISTRY.values():
            if hasattr(model, 'create_index'):
                model.create_index(database=db)


class NodeModel(Base, metaclass=CustomMeta):
    """Base class for Neo4j node models"""

    _default_props: ClassVar[dict] = {}
    _preserve: ClassVar[list[str]] = None
    _append_props: ClassVar[list[str]] = None
    _additional_labels: ClassVar[list[str]] = None
    _labels: ClassVar[list[str]] = []
    _merge_keys: ClassVar[list[str]] = []
    _relationships: ClassVar[dict[str, 'Relationship']] = {}

    model_config = ConfigDict(extra='allow')

    def __init__(self, **data):
        super().__init__(**data)
        for k, v in data.items():
            setattr(self, k, v)
        self._validate_merge_keys()
        self._init_relationships()

    def _init_relationships(self):
        """Initialize relationship instances with parent reference."""
        for rel_name, rel_descriptor in getattr(self.__class__, '_relationships', {}).items():
            # Create a copy of the relationship with this instance as parent
            rel_copy = Relationship(
                source=rel_descriptor.source,
                rel_type=rel_descriptor.rel_type,
                target=rel_descriptor.target,
            )
            rel_copy._parent_instance = self
            # Store directly in __dict__ to be found before the descriptor
            self.__dict__[rel_name] = rel_copy

    @classmethod
    def __init_subclass__(cls, **kwargs):
        super().__init_subclass__(**kwargs)

        # Collect relationships into a class variable
        cls._relationships = {}
        for name, value in cls.__dict__.items():
            if isinstance(value, Relationship):
                cls._relationships[name] = value

                # Optionally add ClassVar annotation if using type hints
                if '__annotations__' in cls.__dict__:
                    cls.__annotations__[name] = ClassVar[Relationship]

    def _validate_merge_keys(self):
        for key in self.__class__._merge_keys:
            if key not in self.__class__.model_fields:
                raise ValueError(f"Merge key '{key}' is not a valid model field.")

    @property
    def _additional_properties(self) -> dict:
        extra_fields = {}
        for extra_field in self.model_fields_set:
            if extra_field not in self.__class__.model_fields:
                extra_fields[extra_field] = getattr(self, extra_field)
        return extra_fields

    @property
    def _all_properties(self) -> dict:
        # get all properties that are not relationships
        properties = {}
        for field in self.__class__.model_fields:
            attr = getattr(self, field)
            if not isinstance(attr, Relationship):
                properties[field] = getattr(self, field)

        properties.update(self._additional_properties)
        return properties

    @classmethod
    def nodeset(cls):
        """Create a NodeSet from this Node."""
        labels = cls._labels
        merge_keys = cls._merge_keys
        default_props = cls._default_props
        preserve = cls._preserve
        append_props = cls._append_props
        additional_labels = cls._additional_labels

        return NodeSet(
            labels=labels,
            merge_keys=merge_keys,
            default_props=default_props,
            preserve=preserve,
            append_props=append_props,
            additional_labels=additional_labels,
        )

    @classmethod
    def dataset(cls):
        return cls.nodeset()

    @classmethod
    def create_index(cls, database=None):
        """Create indexes for this model in the specified database"""
        db = database if database else Base.get_database()
        cls.nodeset().create_index(Base._driver, database=db)

    @property
    def relationships(self) -> list['Relationship']:
        # get all attributes of this class that are instances of Relationship
        relationship_objects = []
        for attr_name, attr_value in self.__dict__.items():
            if attr_name != 'relationships':
                if isinstance(attr_value, Relationship):
                    relationship_objects.append(attr_value)
        return relationship_objects

    @property
    def match_dict(self) -> dict:
        # Get merge keys directly from class.__dict__
        merge_keys = self.__class__.__dict__.get('_merge_keys', [])

        # Build result dictionary
        result = {}
        for key in merge_keys:
            if hasattr(self, key):
                result[key] = getattr(self, key)
        return result

    def create_target_nodes(self, database=None):
        if Base._driver is None:
            raise ValueError('Driver is not set. Use set_driver() to set the driver.')
        for rel in self.relationships:
            if self.__class__.__name__ == rel.source or self.__class__.__name__ == rel.target:
                for other_node, _ in rel.nodes:
                    other_node.create(database=database)

    def create_relationships(self, database=None) -> None:
        """Create relationships for this node."""
        if Base._driver is None:
            raise ValueError('Driver is not set. Use set_driver() to set the driver.')
        db = database if database else Base.get_database()
        for rel in self.relationships:
            # relationships
            if self.__class__.__name__ == rel.source:
                for other_node, properties in rel.nodes:
                    relset = rel.dataset()
                    relset.add_relationship(self.match_dict, other_node.match_dict, properties)
                    relset.create(Base._driver, database=db)
            elif self.__class__.__name__ == rel.target:
                for other_node, properties in rel.nodes:
                    relset = rel.dataset()
                    relset.add_relationship(other_node.match_dict, self.match_dict, properties)
                    relset.create(Base._driver, database=db)

    def create_node(self, database=None):
        if Base._driver is None:
            raise ValueError('Driver is not set. Use set_driver() to set the driver.')
        db = database if database else Base.get_database()
        ns = self.nodeset()

        ns.add_node(self._all_properties)
        ns.create(Base._driver, database=db)

    def create(self, database=None):
        """A full create on a node including relationships and target nodes."""
        self.create_node(database=database)
        self.create_target_nodes(database=database)
        self.create_relationships(database=database)

    def merge_target_nodes(self, database=None):
        if Base._driver is None:
            raise ValueError('Driver is not set. Use set_driver() to set the driver.')
        for rel in self.relationships:
            if self.__class__.__name__ == rel.source or self.__class__.__name__ == rel.target:
                for other_node, _ in rel.nodes:
                    other_node.merge(database=database)

    def merge_relationships(self, database=None) -> None:
        """Merge relationships for this node."""
        if Base._driver is None:
            raise ValueError('Driver is not set. Use set_driver() to set the driver.')
        db = database if database else Base.get_database()
        for rel in self.relationships:
            # relationships
            if self.__class__.__name__ == rel.source:
                relset = rel.dataset()
                for other_node, properties in rel.nodes:
                    relset.add_relationship(self.match_dict, other_node.match_dict, properties)
                relset.merge(Base._driver, database=db)
            elif self.__class__.__name__ == rel.target:
                relset = rel.dataset()
                for other_node, properties in rel.nodes:
                    relset.add_relationship(other_node.match_dict, self.match_dict, properties)
                relset.merge(Base._driver, database=db)

    def merge_node(self, database=None):
        if Base._driver is None:
            raise ValueError('Driver is not set. Use set_driver() to set the driver.')
        db = database if database else Base.get_database()
        ns = self.nodeset()

        ns.add_node(self._all_properties)
        ns.merge(Base._driver, database=db)

    def merge(self, database=None):
        """A full merge on a node including relationships and target nodes."""
        self.merge_node(database=database)
        self.merge_target_nodes(database=database)
        self.merge_relationships(database=database)

    @classmethod
    def match(cls, *filter_ops) -> 'Query':
        """Match nodes using a query builder pattern with deferred execution."""
        # Check if driver is set
        if cls._driver is None:
            raise ValueError('Driver is not set. Use set_driver() to set the driver.')

        # Return unified query builder
        query = Query(cls)
        if filter_ops:
            query = query.filter(*filter_ops)
        return query

    def delete(self, database=None):
        if Base._driver is None:
            raise ValueError('Driver is not set. Use set_driver() to set the driver.')
        db = database if database else Base.get_database()

        query = f"""WITH $properties AS properties
        MATCH (n{get_label_string_from_list_of_labels(self._labels)})
        WHERE {where_clause_with_properties(self.match_dict, 'properties', node_variable='n')}
        DETACH DELETE n
        """

        log.debug(query)
        log.debug(self.match_dict)

        with Base._driver.session(database=db) as session:
            session.run(query, properties=self.match_dict)


class Relationship(BaseModel):
    source: str
    rel_type: str
    target: str
    nodes: list[tuple[Any, dict]] = []

    # Use PrivateAttr for non-model fields
    _parent_instance: Any | None = PrivateAttr(default=None)
    _rel_filters: list[Any] = PrivateAttr(default_factory=list)

    # Add model_config for Pydantic v2
    model_config = {'arbitrary_types_allowed': True}

    def __init__(self, source: str, rel_type: str, target: str, parent=None, **data):
        super().__init__(source=source, rel_type=rel_type, target=target, **data)
        self._parent_instance = parent
        self.nodes = []  # Initialize empty list for each instance

    def __get__(self, instance, owner=None):
        if instance is None:
            # Class access
            return self

        # Instance access
        # Use a unique attribute name for each relationship instance
        inst_attr = f'_rel_{self.rel_type}_{id(self)}'
        if not hasattr(instance, inst_attr):
            # Create instance-specific copy
            rel_copy = self.__class__(
                source=self.source, rel_type=self.rel_type, target=self.target
            )
            rel_copy._parent_instance = instance
            # Store on instance using object.__setattr__ to bypass Pydantic's __setattr__
            object.__setattr__(instance, inst_attr, rel_copy)

        return getattr(instance, inst_attr)

    def add(self, node: Any, properties: dict = None):
        """Add a target node to this relationship"""
        self.nodes.append((node, properties or {}))
        return self  # Allow method chaining

    def dataset(self):
        # Add debugging to see what's in the registry
        base = self._get_base()

        source_node = base.get_class_by_name(self.source)
        target_node = base.get_class_by_name(self.target)

        return RelationshipSet(
            rel_type=self.rel_type,
            start_node_labels=source_node._labels,
            end_node_labels=target_node._labels,
            start_node_properties=source_node._merge_keys,
            end_node_properties=target_node._merge_keys,
        )

    def _get_base(self):
        """Helper method to get the Base class."""
        from graphio.ogm.model import Base

        return Base

    def relationshipset(self):
        return self.dataset()

    def set(self):
        return self.dataset()

    def filter(self, *filter_ops):
        """
        Filter relationships based on relationship properties and return a query builder.

        Usage:
        person.knows.filter(RelField("score") > 90).match(Person.age > 50)

        :param filter_ops: FilterOp objects for complex filtering on relationship properties
        :return: Query with relationship filters applied
        """
        # Get the base class to access driver and registry
        base = self._get_base()
        if not base:
            raise ValueError('Could not determine Base class')

        source_class = base.get_class_by_name(self.source)
        target_class = base.get_class_by_name(self.target)

        # Detect if this is a reverse relationship
        # For self-referencing relationships (source == target), always use normal direction
        if self._parent_instance:
            parent_class_name = self._parent_instance.__class__.__name__
            is_reverse = parent_class_name == self.target and self.source != self.target

            if is_reverse:
                # Swap source and target for reverse relationships
                query_source_class = target_class
                query_target_class = source_class
            else:
                query_source_class = source_class
                query_target_class = target_class
        else:
            # For class-level queries, assume normal direction
            query_source_class = source_class
            query_target_class = target_class
            is_reverse = False

        # Start with existing rel_filters plus new ones
        rel_filters = list(getattr(self, '_rel_filters', []))
        for f in filter_ops:
            if isinstance(f, FilterOp):
                rel_filters.append(f)
            else:
                raise ValueError(f'Unsupported filter type: {type(f)}')

        # Create 1-hop query path with relationship filters
        path = [
            PathStep(query_source_class),
            PathStep(
                query_target_class, rel_type=(self.rel_type, is_reverse), rel_filters=rel_filters
            ),
        ]

        return Query(query_source_class, source_instance=self._parent_instance, path=path)

    def match(self, *filter_ops):
        """
        Match and return instances of the target node with filtering capabilities.
        Returns a query builder for deferred execution.

        Usage:
        # Get all related nodes
        all_friends = person.knows.match().all()

        # Get the first related node that matches criteria
        best_friend = person.knows.filter(RelField("score") == 100).match(Person.age > 25).first()
        """
        # Get the base class to access driver and registry
        base = self._get_base()
        if not base:
            raise ValueError('Could not determine Base class')

        source_class = base.get_class_by_name(self.source)
        target_class = base.get_class_by_name(self.target)

        # Detect if this is a reverse relationship
        # For self-referencing relationships (source == target), always use normal direction
        if self._parent_instance:
            parent_class_name = self._parent_instance.__class__.__name__
            is_reverse = parent_class_name == self.target and self.source != self.target

            if is_reverse:
                # Swap source and target for reverse relationships
                query_source_class = target_class
                query_target_class = source_class
            else:
                query_source_class = source_class
                query_target_class = target_class
        else:
            # For class-level queries, assume normal direction
            query_source_class = source_class
            query_target_class = target_class
            is_reverse = False

        # Create 1-hop query path: source -> relationship -> target
        path = [
            PathStep(query_source_class),
            PathStep(
                query_target_class,
                rel_type=(self.rel_type, is_reverse),
                rel_filters=getattr(self, '_rel_filters', []),
            ),
        ]

        # Create query with instance context and add any node filters
        query = Query(query_source_class, source_instance=self._parent_instance, path=path)
        if filter_ops:
            query = query.filter(*filter_ops)
        return query

    def delete(self, target=None, database=None):
        """
        Delete all relationships of this type between the source and target nodes.
        """
        base = self._get_base()
        if not base:
            raise ValueError('Could not determine Base class')
        driver = base.get_driver()
        db = database if database else base.get_database()
        target_class = base.get_class_by_name(self.target)

        # if target instance is provided, delete only the relationship between the source and target
        if target:
            query = f"""WITH $properties AS properties, $target_properties AS target_properties
            MATCH (source{get_label_string_from_list_of_labels(self._parent_instance._labels)})-[r:{self.rel_type}]->(target{get_label_string_from_list_of_labels(target_class._labels)})
            WHERE {where_clause_with_properties(self._parent_instance.match_dict, 'properties', node_variable='source')} \n"""
            if target:
                query += f' AND {where_clause_with_properties(target.match_dict, "target_properties", node_variable="target")} \n'
            query += 'DELETE r'
            log.debug(query)

            with driver.session(database=db) as session:
                session.run(
                    query,
                    properties=self._parent_instance.match_dict,
                    target_properties=target.match_dict,
                )

        else:
            query = f"""WITH $properties AS properties
            MATCH (source{get_label_string_from_list_of_labels(self._parent_instance._labels)})-[r:{self.rel_type}]->(target{get_label_string_from_list_of_labels(target_class._labels)})
            WHERE {where_clause_with_properties(self._parent_instance.match_dict, 'properties', node_variable='source')}
            DELETE r
            """
            log.debug(query)

            with driver.session(database=db) as session:
                session.run(query, properties=self._parent_instance.match_dict)
