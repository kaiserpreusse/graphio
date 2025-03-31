import importlib
import sys
import pkgutil
import inspect
import logging
from typing import List, ClassVar, Dict, Any, Optional, Type, Union, Set, Tuple, TypeVar, Generic

from neo4j import Driver
from pydantic import BaseModel, PrivateAttr

from graphio import NodeSet, RelationshipSet
from graphio.queries import where_clause_with_properties
from graphio.helper import convert_neo4j_types_to_python

log = logging.getLogger(__name__)

# Type variables for better typing
T = TypeVar('T', bound='NodeModel')

_GLOBAL_REGISTRY = None


def get_global_registry():
    global _GLOBAL_REGISTRY
    if _GLOBAL_REGISTRY is None:
        _GLOBAL_REGISTRY = Registry()
    return _GLOBAL_REGISTRY


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
            return QueryField(self.name, owner)
        # Instance access - pass through to normal attribute access
        return instance.__dict__.get(self.name)


class QueryField:
    """Helper class for creating filter operations with class field reference syntax"""

    def __init__(self, field_name, model_cls):
        self.field_name = field_name
        self.model_cls = model_cls

    def __eq__(self, other):
        return FilterOp(self.field_name, "=", other)

    def __gt__(self, other):
        return FilterOp(self.field_name, ">", other)

    def __lt__(self, other):
        return FilterOp(self.field_name, "<", other)

    def __ge__(self, other):
        return FilterOp(self.field_name, ">=", other)

    def __le__(self, other):
        return FilterOp(self.field_name, "<=", other)

    def __ne__(self, other):
        return FilterOp(self.field_name, "<>", other)

    def starts_with(self, value):
        return FilterOp(self.field_name, "STARTS WITH", value)

    def ends_with(self, value):
        return FilterOp(self.field_name, "ENDS WITH", value)

    def contains(self, value):
        return FilterOp(self.field_name, "CONTAINS", value)


class RelField:
    """Helper class for creating filter operations on relationship properties"""
    def __init__(self, field_name):
        self.field_name = field_name

    def __eq__(self, other):
        return FilterOp(self.field_name, "=", other)

    def __gt__(self, other):
        return FilterOp(self.field_name, ">", other)

    def __lt__(self, other):
        return FilterOp(self.field_name, "<", other)

    def __ge__(self, other):
        return FilterOp(self.field_name, ">=", other)

    def __le__(self, other):
        return FilterOp(self.field_name, "<=", other)

    def __ne__(self, other):
        return FilterOp(self.field_name, "<>", other)

    def starts_with(self, value):
        return FilterOp(self.field_name, "STARTS WITH", value)

    def ends_with(self, value):
        return FilterOp(self.field_name, "ENDS WITH", value)

    def contains(self, value):
        return FilterOp(self.field_name, "CONTAINS", value)

class Registry:
    def __init__(self):
        self.default = []
        self._is_initialized = False

    def __iter__(self):
        return iter(self.default)

    def add(self, cls):
        # Check if the class is already in the registry by class object (not just name)
        if cls not in self.default:
            self.default.append(cls)
            return True
        return False

    def auto_discover(self):
        """Auto-discover all model classes in the caller's module and related modules"""
        if self._is_initialized:
            return

        # Get the frame that called this method
        frame = inspect.currentframe().f_back.f_back
        module = inspect.getmodule(frame)

        if not module:
            return

        module_name = module.__name__

        # Track all modules we've scanned
        scanned_modules = set()

        # Scan the caller's module first
        self._scan_module_for_models(module, scanned_modules)

        # Import common related modules
        try:
            # Try to import a 'model' or 'models' module in the same package
            package_name = module.__name__.split('.')[0]
            for related_name in ['model', 'models']:
                try:
                    related_module = importlib.import_module(f"{package_name}.{related_name}")
                    self._scan_module_for_models(related_module, scanned_modules)
                except ImportError:
                    try:
                        # Try as a direct import
                        related_module = importlib.import_module(related_name)
                        self._scan_module_for_models(related_module, scanned_modules)
                    except ImportError:
                        pass

            # Import the package itself if it's not already imported
            try:
                package = importlib.import_module(package_name)
                self._scan_module_for_models(package, scanned_modules)
            except ImportError:
                pass

            # If it's a package, import all submodules
            if hasattr(sys.modules[module_name], '__path__'):
                pkg = sys.modules[module_name]
                for _, name, is_pkg in pkgutil.iter_modules(pkg.__path__, pkg.__name__ + '.'):
                    try:
                        imported_module = importlib.import_module(name)
                        self._scan_module_for_models(imported_module, scanned_modules)
                    except ImportError as e:
                        log.warning(f"Could not import {name}: {e}")
        except (KeyError, AttributeError) as e:
            log.warning(f"Error during module discovery: {e}")

        self._is_initialized = True

    def _scan_module_for_models(self, module, scanned_modules=None):
        """Scan a module for model classes and add them to the registry"""
        if scanned_modules is None:
            scanned_modules = set()

        if module.__name__ in scanned_modules:
            return

        scanned_modules.add(module.__name__)

        # Find and register all classes with _labels and _merge_keys
        for name, obj in inspect.getmembers(module):
            if inspect.isclass(obj) and hasattr(obj, '_labels') and hasattr(obj, '_merge_keys'):
                # Get reference to the Base class through the MRO
                base_class = None
                for parent in obj.__mro__:
                    if hasattr(parent, 'get_registry') and hasattr(parent, 'NodeModel'):
                        base_class = parent
                        break

                if base_class:
                    # Use Base's registry directly
                    registry = base_class.get_registry()
                    registry.add(obj)
                else:
                    # Fall back to adding directly to this registry
                    self.add(obj)


class CustomMeta(BaseModel.__class__):
    def __new__(mcs, name, bases, attrs):
        # First create the class using the normal mechanism
        cls = super().__new__(mcs, name, bases, attrs)

        # Add field descriptors for all fields in model classes
        if name not in ('Base', 'NodeModel', 'RelationshipModel'):
            if hasattr(cls, 'model_fields'):
                for field_name in cls.model_fields:
                    setattr(cls, field_name, QueryFieldDescriptor(field_name))

        # Register class (existing code)
        if name not in ('Base', 'NodeModel', 'RelationshipModel'):
            if hasattr(cls, '_labels') and hasattr(cls, '_merge_keys'):
                registry = get_global_registry()
                registry.add(cls)

        return cls


class NodeQueryBuilder:
    """Builder class for node queries with deferred execution."""

    def __init__(self, node_class, filter_ops=None, equality_filters=None):
        self.node_class = node_class
        self.filter_ops = filter_ops or []
        self.equality_filters = equality_filters or {}

    def __getattr__(self, name):
        """Allow access to relationships like Person.match().knows"""
        # Check if this is a relationship defined on the class
        if hasattr(self.node_class, '_relationships') and name in self.node_class._relationships:
            # Return a relationship query builder
            relationship = self.node_class._relationships[name]
            return ChainedRelationshipQueryBuilder(
                self.node_class,
                relationship.rel_type,
                relationship.target,
                self.filter_ops,
                self.equality_filters
            )
        raise AttributeError(f"'{self.node_class.__name__}' has no relationship '{name}'")

    def all(self):
        """Execute the query and return all results."""
        if self.node_class._driver is None:
            raise ValueError("Driver is not set. Use set_driver() to set the driver.")

        # Handle CypherQuery if present
        for f in self.filter_ops:
            if isinstance(f, CypherQuery):
                return self._execute_cypher_query(f)

        # Build and execute regular query
        query = f"""MATCH (n{self.node_class._label_match_string()})"""
        conditions = []
        params = {}

        # Process equality filters from kwargs
        for field, value in self.equality_filters.items():
            param_name = f"{field}_eq"
            conditions.append(f"n.{field} = ${param_name}")
            params[param_name] = value

        # Process FilterOp objects
        for i, f in enumerate(self.filter_ops):
            param_name = f"{f.field}_{i}"
            conditions.append(f"n.{f.field} {f.operator} ${param_name}")
            params[param_name] = f.value

        if conditions:
            query += f"\nWHERE {' AND '.join(conditions)}"

        query += "\nRETURN n"
        log.debug(query)
        # Execute the query
        nodes = []
        with self.node_class._driver.session() as session:
            result = session.run(query, params)
            for record in result:
                node = record['n']
                properties = dict(node.items())
                properties = convert_neo4j_types_to_python(properties)
                nodes.append(self.node_class(**properties))

        return nodes

    def first(self):
        """Execute the query with LIMIT 1."""
        if self.node_class._driver is None:
            raise ValueError("Driver is not set. Use set_driver() to set the driver.")
        # Handle CypherQuery if present
        for f in self.filter_ops:
            if isinstance(f, CypherQuery):
                # TODO: For custom CypherQuery objects, we should ideally add LIMIT 1
                # For now, just execute the same code as all() but return only first result
                log.warning(f"Using CypherQuery in first() method needs a LIMIT 1 in the query. ")
                results = self._execute_cypher_query(f)
                return results[0] if results else None

        # Build query (same as in all() method)
        query = f"""MATCH (n{self.node_class._label_match_string()})"""
        conditions = []
        params = {}

        # Process equality filters
        for field, value in self.equality_filters.items():
            param_name = f"{field}_eq"
            conditions.append(f"n.{field} = ${param_name}")
            params[param_name] = value

        # Process FilterOp objects
        for i, f in enumerate(self.filter_ops):
            param_name = f"{f.field}_{i}"
            conditions.append(f"n.{f.field} {f.operator} ${param_name}")
            params[param_name] = f.value

        if conditions:
            query += f"\nWHERE {' AND '.join(conditions)}"

        # Add LIMIT 1 to only return a single node
        query += "\nRETURN n LIMIT 1"

        # Execute the query
        with self.node_class._driver.session() as session:
            result = session.run(query, params)
            record = result.single()  # Get only one record
            if record and 'n' in record.keys():
                node = record['n']
                properties = dict(node.items())
                properties = convert_neo4j_types_to_python(properties)
                return self.node_class(**properties)

        return None

    def _execute_cypher_query(self, cypher_query):
        """Execute a CypherQuery and return results."""
        nodes = []
        with self.node_class._driver.session() as session:
            result = session.run(cypher_query.query, cypher_query.params)
            for record in result:
                if 'n' not in record.keys():
                    raise ValueError(f"Query must return nodes with variable name 'n'. Got keys: {record.keys()}")

                node = record['n']
                properties = dict(node.items())
                properties = convert_neo4j_types_to_python(properties)
                nodes.append(self.node_class(**properties))

        return nodes


class ChainedRelationshipQueryBuilder:
    """Builder class for chained relationship queries."""

    def __init__(self, source_class, rel_type, target_class_name,
                 source_filters, source_equality_filters):
        self.source_class = source_class
        self.rel_type = rel_type
        self.target_class_name = target_class_name
        self.source_filters = source_filters
        self.source_equality_filters = source_equality_filters
        self.rel_filters = {}

    def filter(self, *filter_ops, **equality_filters):
        """Filter relationships based on relationship properties."""
        new_builder = ChainedRelationshipQueryBuilder(
            self.source_class,
            self.rel_type,
            self.target_class_name,
            self.source_filters,
            self.source_equality_filters
        )
        new_builder.rel_filters = self.rel_filters.copy()

        # Add equality filters
        for field, value in equality_filters.items():
            new_builder.rel_filters[field] = FilterOp(field, "=", value)

        # Add FilterOp objects
        for f in filter_ops:
            if isinstance(f, FilterOp):
                new_builder.rel_filters[f.field] = f

        return new_builder

    def match(self, *filter_ops, **equality_filters):
        """Match nodes related to nodes of this class."""
        # Get the target class directly from Base
        from graphio.objects.model import Base
        target_class = Base.get_class_by_name(self.target_class_name)
        if not target_class:
            raise ValueError(f"Could not find target class {self.target_class_name}")

        return ChainedRelationshipMatchBuilder(
            self.source_class,
            self.rel_type,
            target_class,
            self.source_filters,
            self.source_equality_filters,
            self.rel_filters,
            filter_ops,
            equality_filters
        )


class ChainedRelationshipMatchBuilder:
    """Builder class for executing chained relationship queries."""

    def __init__(self, source_class, rel_type, target_class,
                 source_filters, source_equality_filters,
                 rel_filters, target_filters=None, target_equality_filters=None):
        self.source_class = source_class
        self.rel_type = rel_type
        self.target_class = target_class
        self.source_filters = source_filters
        self.source_equality_filters = source_equality_filters
        self.rel_filters = rel_filters or {}
        self.target_filters = target_filters or []
        self.target_equality_filters = target_equality_filters or {}

    def _build_query(self, limit=None):
        """Build a Cypher query with all filters applied."""
        query = f"""
        MATCH (source{self.source_class._label_match_string()})-[r:{self.rel_type}]->(target{self.target_class._label_match_string()})
        """

        conditions = []
        params = {}

        # Source node filters
        for field, value in self.source_equality_filters.items():
            param_name = f"source_{field}_eq"
            conditions.append(f"source.{field} = ${param_name}")
            params[param_name] = value

        for i, f in enumerate(self.source_filters):
            param_name = f"source_{f.field}_{i}"
            conditions.append(f"source.{f.field} {f.operator} ${param_name}")
            params[param_name] = f.value

        # Relationship filters
        for i, (field, filter_op) in enumerate(self.rel_filters.items()):
            param_name = f"rel_{field}_{i}"
            conditions.append(f"r.{filter_op.field} {filter_op.operator} ${param_name}")
            params[param_name] = filter_op.value

        # Target node filters
        for field, value in self.target_equality_filters.items():
            param_name = f"target_{field}_eq"
            conditions.append(f"target.{field} = ${param_name}")
            params[param_name] = value

        for i, f in enumerate(self.target_filters):
            param_name = f"target_{f.field}_{i}"
            conditions.append(f"target.{f.field} {f.operator} ${param_name}")
            params[param_name] = f.value

        if conditions:
            query += f"WHERE {' AND '.join(conditions)}\n"

        query += "RETURN DISTINCT target"

        if limit is not None:
            query += f" LIMIT {limit}"

        return query, params

    def all(self):
        """Execute the query and return all targets."""
        driver = self.source_class.get_driver()

        query, params = self._build_query()

        # Execute the query
        results = []
        with driver.session() as session:
            result = session.run(query, params)
            for record in result:
                node = record['target']
                properties = dict(node.items())
                properties = convert_neo4j_types_to_python(properties)
                results.append(self.target_class(**properties))

        return results

    def first(self):
        """Execute the query and return the first target."""
        driver = self.source_class.get_driver()
        query, params = self._build_query(limit=1)

        # Execute the query
        with driver.session() as session:
            result = session.run(query, params)
            record = result.single()
            if record and 'target' in record.keys():
                node = record['target']
                properties = dict(node.items())
                properties = convert_neo4j_types_to_python(properties)
                return self.target_class(**properties)

        return None


class InstanceRelationshipQueryBuilder:
    """Builder class for instance-level relationship queries with deferred execution."""

    def __init__(self, parent_instance, rel_type, target_class, rel_filters=None,
                 node_filters=None, node_equality_filters=None):
        self.parent_instance = parent_instance
        self.rel_type = rel_type
        self.target_class = target_class
        self.rel_filters = rel_filters or {}
        self.node_filters = node_filters or []
        self.node_equality_filters = node_equality_filters or {}

    def _build_query(self, limit=None):
        """Build the Cypher query with optional LIMIT clause."""
        base = self._get_base()

        query = f"""WITH $properties AS properties
        MATCH (source{self.parent_instance._label_match_string()})-[r:{self.rel_type}]->(target{self.target_class._label_match_string()})
        WHERE {where_clause_with_properties(self.parent_instance.match_dict, 'properties', node_variable='source')}"""

        conditions = []
        params = {"properties": self.parent_instance.match_dict}

        # Add relationship property filters
        for i, (field, filter_op) in enumerate(self.rel_filters.items()):
            param_name = f"rel_{field}_{i}"
            conditions.append(f"r.{filter_op.field} {filter_op.operator} ${param_name}")
            params[param_name] = filter_op.value

        # Add node property filters
        for field, value in self.node_equality_filters.items():
            param_name = f"{field}_eq"
            conditions.append(f"target.{field} = ${param_name}")
            params[param_name] = value

        for i, f in enumerate(self.node_filters):
            param_name = f"{f.field}_{i}"
            conditions.append(f"target.{f.field} {f.operator} ${param_name}")
            params[param_name] = f.value

        if conditions:
            query += f"\nAND {' AND '.join(conditions)}"

        query += "\nRETURN DISTINCT target"

        if limit is not None:
            query += f" LIMIT {limit}"

        return query, params

    def all(self):
        """Execute the query and return all target nodes."""
        base = self._get_base()
        driver = base.get_driver()

        query, params = self._build_query()

        # Execute the query
        instances = []
        with driver.session() as session:
            result = session.run(query, **params)
            for record in result:
                node = record['target']
                properties = dict(node.items())
                properties = convert_neo4j_types_to_python(properties)
                instances.append(self.target_class(**properties))

        return instances

    def first(self):
        """Execute the query and return only the first target node."""
        base = self._get_base()
        driver = base.get_driver()

        query, params = self._build_query(limit=1)
        print(f"In first() method in InstanceRelationshipQueryBuilder")
        print(query)
        print(params)

        # Execute the query
        with driver.session() as session:
            result = session.run(query, **params)
            record = result.single()
            if record and 'target' in record.keys():
                node = record['target']
                properties = dict(node.items())
                properties = convert_neo4j_types_to_python(properties)
                return self.target_class(**properties)

        return None

    def _get_base(self):
        """Helper method to get the Base class."""
        from graphio.objects.model import Base
        return Base


class Base(BaseModel, metaclass=CustomMeta):
    """Static base class for all Neo4j ORM models"""
    _driver = None

    @classmethod
    def discover_models(cls):
        """Scan modules to discover model classes"""
        cls.get_registry().auto_discover()
        return cls

    @classmethod
    def get_registry(cls):
        return get_global_registry()

    @classmethod
    def set_driver(cls, driver: Driver):
        cls._driver = driver
        return cls

    @classmethod
    def get_driver(cls) -> Driver:
        if cls._driver is None:
            raise ValueError("Driver is not set. Use set_driver() to set the driver.")
        return cls._driver

    @classmethod
    def get_class_by_name(cls, name):
        """Get a model class by its name"""
        registry = cls.get_registry()
        for subclass in registry:
            if subclass.__name__ == name:
                return subclass
        return None

    @classmethod
    def model_create_index(cls):
        """Create indexes for all models"""
        registry = cls.get_registry()
        for model in registry:
            if hasattr(model, 'create_index'):
                model.create_index()


class NodeModel(Base, metaclass=CustomMeta):
    """Base class for Neo4j node models"""
    _default_props: ClassVar[Dict] = {}
    _preserve: ClassVar[List[str]] = None
    _append_props: ClassVar[List[str]] = None
    _additional_labels: ClassVar[List[str]] = None
    _labels: ClassVar[List[str]] = []
    _merge_keys: ClassVar[List[str]] = []
    _relationships: ClassVar[Dict[str, 'Relationship']] = {}

    class Config:
        extra = 'allow'

    def __init__(self, **data):
        super().__init__(**data)
        for k, v in data.items():
            setattr(self, k, v)
        self._validate_merge_keys()

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
            if key not in self.model_fields:
                raise ValueError(f"Merge key '{key}' is not a valid model field.")

    @property
    def _additional_properties(self) -> dict:
        extra_fields = {}
        for extra_field in self.model_fields_set:
            if extra_field not in self.model_fields:
                extra_fields[extra_field] = getattr(self, extra_field)
        return extra_fields

    @property
    def _all_properties(self) -> dict:
        # get all properties that are not relationships
        properties = {}
        for field in self.model_fields:
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

        return NodeSet(labels=labels, merge_keys=merge_keys, default_props=default_props,
                       preserve=preserve, append_props=append_props, additional_labels=additional_labels)

    @classmethod
    def dataset(cls):
        return cls.nodeset()

    @classmethod
    def create_index(cls):
        cls.nodeset().create_index(Base._driver)

    @property
    def relationships(self) -> List['Relationship']:
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

    @property
    def _unique_id_dict(self) -> dict:
        """Return a dictionary of the merge keys and values to identify this node uniquely."""
        result = {}
        for k in self.__class__.__dict__.get('_merge_keys', []):
            if hasattr(self, k):
                result[k] = getattr(self, k)
        return result

    def create_target_nodes(self):
        if Base._driver is None:
            raise ValueError("Driver is not set. Use set_driver() to set the driver.")
        for rel in self.relationships:
            if self.__class__.__name__ == rel.source or self.__class__.__name__ == rel.target:
                for other_node, properties in rel.nodes:
                    other_node.create()

    def create_relationships(self) -> None:
        """Create relationships for this node."""
        if Base._driver is None:
            raise ValueError("Driver is not set. Use set_driver() to set the driver.")
        for rel in self.relationships:
            # relationships
            if self.__class__.__name__ == rel.source:
                for other_node, properties in rel.nodes:
                    relset = rel.dataset()
                    relset.add_relationship(self.match_dict, other_node.match_dict, properties)
                    relset.create(Base._driver)
            elif self.__class__.__name__ == rel.target:
                for other_node, properties in rel.nodes:
                    relset = rel.dataset()
                    relset.add_relationship(other_node.match_dict, self.match_dict, properties)
                    relset.create(Base._driver)

    def create_node(self):
        if Base._driver is None:
            raise ValueError("Driver is not set. Use set_driver() to set the driver.")
        ns = self.nodeset()

        ns.add_node(self._all_properties)
        ns.create(Base._driver)

    def create(self):
        """A full create on a node including relationships and target nodes."""
        self.create_node()
        self.create_target_nodes()
        self.create_relationships()

    def merge_target_nodes(self):
        if Base._driver is None:
            raise ValueError("Driver is not set. Use set_driver() to set the driver.")
        for rel in self.relationships:
            if self.__class__.__name__ == rel.source or self.__class__.__name__ == rel.target:
                for other_node, properties in rel.nodes:
                    other_node.merge()

    def merge_relationships(self) -> None:
        """Merge relationships for this node."""
        if Base._driver is None:
            raise ValueError("Driver is not set. Use set_driver() to set the driver.")
        for rel in self.relationships:
            # relationships
            if self.__class__.__name__ == rel.source:
                relset = rel.dataset()
                for other_node, properties in rel.nodes:
                    relset.add_relationship(self.match_dict, other_node.match_dict, properties)
                relset.merge(Base._driver)
            elif self.__class__.__name__ == rel.target:
                relset = rel.dataset()
                for other_node, properties in rel.nodes:
                    relset.add_relationship(other_node.match_dict, self.match_dict, properties)
                relset.merge(Base._driver)

    def merge_node(self):
        if Base._driver is None:
            raise ValueError("Driver is not set. Use set_driver() to set the driver.")
        ns = self.nodeset()

        ns.add_node(self._all_properties)
        ns.merge(Base._driver)

    def merge(self):
        """A full merge on a node including relationships and target nodes."""
        self.merge_node()
        self.merge_target_nodes()
        self.merge_relationships()

    @classmethod
    def _label_match_string(cls):
        return ":" + ":".join(cls._labels)

    @classmethod
    def match(cls, *filter_ops, **equality_filters) -> 'NodeQueryBuilder[T]':
        """Match nodes using a query builder pattern with deferred execution."""
        # Check if driver is set
        if cls._driver is None:
            raise ValueError("Driver is not set. Use set_driver() to set the driver.")

        # Check if kwargs are valid model fields
        for key in equality_filters.keys():
            if key not in cls.model_fields:
                log.warning(f"Key '{key}' is not a valid model field. The result will not "
                            f"be accessible as a model instance attribute.")

        # Return a query builder instead of executing directly
        return NodeQueryBuilder(cls, filter_ops, equality_filters)

    def delete(self):
        if Base._driver is None:
            raise ValueError("Driver is not set. Use set_driver() to set the driver.")

        query = f"""WITH $properties AS properties
        MATCH (n{self._label_match_string()})
        WHERE {where_clause_with_properties(self.match_dict, 'properties', node_variable='n')}
        DETACH DELETE n
        """

        log.debug(query)
        log.debug(self._unique_id_dict)

        with Base._driver.session() as session:
            session.run(query, properties=self._unique_id_dict)


class RelationshipModel(Base, metaclass=CustomMeta):
    """Base class for Neo4j relationship models"""
    source: ClassVar[str]
    target: ClassVar[str]
    rel_type: ClassVar[str]
    default_props: ClassVar[Dict[str, Any]] = {}

    @classmethod
    def create_index(cls):
        # Will be implemented later
        pass


class Relationship(BaseModel):
    source: str
    rel_type: str
    target: str
    nodes: List[Tuple[Any, Dict]] = []

    # Use PrivateAttr for non-model fields
    _parent_instance: Optional[Any] = PrivateAttr(default=None)
    _rel_filters: Dict[str, Any] = PrivateAttr(default_factory=dict)

    # Add model_config for Pydantic v2
    model_config = {
        "arbitrary_types_allowed": True
    }

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
        inst_attr = f"_rel_{self.rel_type}_{id(self)}"
        if not hasattr(instance, inst_attr):
            # Create instance-specific copy
            rel_copy = self.__class__(
                source=self.source,
                rel_type=self.rel_type,
                target=self.target
            )
            rel_copy._parent_instance = instance
            # Store on instance using object.__setattr__ to bypass Pydantic's __setattr__
            object.__setattr__(instance, inst_attr, rel_copy)

        return getattr(instance, inst_attr)

    def add(self, node: Any, properties: Dict = None):
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
        from graphio.objects.model import Base
        return Base

    def relationshipset(self):
        return self.dataset()

    def set(self):
        return self.dataset()

    def filter(self, *filter_ops, **equality_filters):
        """
        Filter relationships based on relationship properties.

        Usage:
        Person.knows.filter(RelField("score") > 90).match(Person.age > 50)
        Person.knows.filter(score="good").match(Person.age > 50)

        :param filter_ops: FilterOp objects for complex filtering on relationship properties
        :param equality_filters: Keyword arguments for equality conditions on relationship properties
        :return: self (for method chaining)
        """
        # Create a new instance of the relationship with filters
        rel_copy = self.__class__(
            source=self.source,
            rel_type=self.rel_type,
            target=self.target
        )
        rel_copy._parent_instance = self._parent_instance
        rel_copy.nodes = self.nodes.copy()

        # Store relationship filters
        rel_copy._rel_filters = {**getattr(self, '_rel_filters', {})}  # Copy existing filters

        # Add equality filters from kwargs
        for field, value in equality_filters.items():
            rel_copy._rel_filters[field] = FilterOp(field, "=", value)

        # Add FilterOp objects
        for f in filter_ops:
            if isinstance(f, FilterOp):
                rel_copy._rel_filters[f.field] = f

        return rel_copy

    def match(self, *filter_ops, **equality_filters):
        """
        Match and return instances of the target node with filtering capabilities.
        Returns a query builder for deferred execution.

        Usage:
        # Get all related nodes
        all_friends = person.knows.match().all()

        # Get the first related node that matches criteria
        best_friend = person.knows.filter(score=100).match(name="Bob").first()
        """
        # Get the base class to access driver and registry
        base = self._get_base()
        if not base:
            raise ValueError("Could not determine Base class")

        target_class = base.get_class_by_name(self.target)

        # Return a query builder instead of executing directly
        return InstanceRelationshipQueryBuilder(
            parent_instance=self._parent_instance,
            rel_type=self.rel_type,
            target_class=target_class,
            rel_filters=getattr(self, '_rel_filters', {}),
            node_filters=filter_ops,
            node_equality_filters=equality_filters
        )

    def delete(self, target=None):
        """
        Delete all relationships of this type between the source and target nodes.
        """
        base = self._get_base()
        if not base:
            raise ValueError("Could not determine Base class")
        driver = base.get_driver()
        target_class = base.get_class_by_name(self.target)

        # if target instance is provided, delete only the relationship between the source and target
        if target:

            query = f"""WITH $properties AS properties, $target_properties AS target_properties
            MATCH (source{self._parent_instance._label_match_string()})-[r:{self.rel_type}]->(target{target_class._label_match_string()})
            WHERE {where_clause_with_properties(self._parent_instance.match_dict, 'properties', node_variable='source')} \n"""
            if target:
                query += f" AND {where_clause_with_properties(target.match_dict, 'target_properties', node_variable='target')} \n"
            query += "DELETE r"
            log.debug(query)

            with driver.session() as session:
                session.run(query, properties=self._parent_instance.match_dict, target_properties=target.match_dict)

        else:
            query = f"""WITH $properties AS properties
            MATCH (source{self._parent_instance._label_match_string()})-[r:{self.rel_type}]->(target{target_class._label_match_string()})
            WHERE {where_clause_with_properties(self._parent_instance.match_dict, 'properties', node_variable='source')}
            DELETE r
            """
            log.debug(query)

            with driver.session() as session:
                session.run(query, properties=self._parent_instance.match_dict)


class Graph(BaseModel):
    def create(self, *objects):
        for o in objects:
            if hasattr(o, 'create_node'):
                o.create_node()
        for o in objects:
            if hasattr(o, 'create_relationships'):
                o.create_relationships()

    def merge(self, *objects):
        for o in objects:
            if hasattr(o, 'merge_node'):
                o.merge_node()
        for o in objects:
            if hasattr(o, 'merge_relationships'):
                o.merge_relationships()
