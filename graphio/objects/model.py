import importlib
import sys
import pkgutil
import inspect
import logging
from typing import List, ClassVar, Dict, Any, Optional, Type, Union, Set, Tuple

from pydantic import BaseModel, PrivateAttr

from graphio import NodeSet, RelationshipSet
from graphio.queries import where_clause_with_properties
from graphio.helper import convert_neo4j_types_to_python

log = logging.getLogger(__name__)

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


def declarative_base():
    """
    Create a declarative base for Neo4j model definitions.
    Similar to SQLAlchemy's declarative_base but works with Pydantic.
    """

    # Create the base class
    class Base(BaseModel, metaclass=CustomMeta):
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
        def set_driver(cls, driver):
            cls._driver = driver
            return cls

        @classmethod
        def get_driver(cls):
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

    # Create the node model class with all functionality from _NodeModel
    class NodeModel(Base, metaclass=CustomMeta):
        """Base class for Neo4j node models"""
        _default_props: ClassVar[Dict] = {}
        _preserve: ClassVar[List[str]] = None
        _append_props: ClassVar[List[str]] = None
        _additional_labels: ClassVar[List[str]] = None
        _labels: ClassVar[List[str]] = []
        _merge_keys: ClassVar[List[str]] = []

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
            """
            Create a NodeSet from this Node.

            :return: NodeSet
            """
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
            """
            Return a dictionary of the merge keys and values to identify this node uniquely.

            :return: dict with merge keys and values
            """
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
            """
            Create relationships for this node.
            """
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
            """
            A full create on a node including relationships and target nodes.

            In most cases it's better to use a container class that manages the
            creation of all nodes and relationships. In principle, this method
            walks down a chain of nodes but that's difficult to handle from code.
            """
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
            """
            Merge relationships for this node.
            """
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
            """
            A full merge on a node including relationships and target nodes.

            In most cases it's better to use a container class that manages the
            creation of all nodes and relationships. In principle, this method
            walks down a chain of nodes but that's difficult to handle from code.
            """
            self.merge_node()
            self.merge_target_nodes()
            self.merge_relationships()

        @classmethod
        def _label_match_string(cls):
            return ":" + ":".join(cls._labels)

        @classmethod
        def match(cls, *filter_ops, **equality_filters) -> List['NodeModel']:
            """
            Match and return instances of this NodeModel with flexible filtering.

            1. FilterOp objects
            2. FieldReference objects (converted to FilterOp)
            3. CypherQuery objects for raw Cypher
            4. Keyword arguments for simple equality filtering

            Usage:
            # All nodes of a type
            all_persons = Person.match()

            # Equality filtering with keyword arguments
            johns = Person.match(name="John")

            # Comparison operations with FilterOp
            adults = Person.match(FilterOp("age", ">", 30))

            # Combined equality and comparison filtering
            johns_over_30 = Person.match(FilterOp("age", ">", 30), name="John")

            :param filter_ops: FilterOp objects for complex filtering
            :param equality_filters: Keyword arguments for equality conditions
            :return: List of NodeModel instances matching the conditions
            """
            if Base._driver is None:
                raise ValueError("Driver is not set. Use set_driver() to set the driver.")

            # Cypher query first:
            # Check if a CypherQuery is being used
            for f in filter_ops:
                if isinstance(f, CypherQuery):
                    log.debug(f.query)
                    log.debug(f.params)
                    nodes = []

                    with Base._driver.session() as session:
                        result = session.run(f.query, f.params)
                        for record in result:
                            # Check if the record has a node with key 'n'
                            if 'n' not in record.keys():
                                raise ValueError(
                                    f"Query must return nodes with variable name 'n'. Got keys: {record.keys()}")

                            node = record.get('n')
                            properties = dict(node.items())

                            # Convert Neo4j types to Python types
                            properties = convert_neo4j_types_to_python(properties)
                            nodes.append(cls.model_construct(**properties))

                    return nodes

            # Check if kwargs are valid model fields
            for key in equality_filters.keys():
                if key not in cls.model_fields:
                    log.warning(f"Key '{key}' is not a valid model field. We try to match but the result will not "
                                f"be accessible as a model instance attribute.")

            query = f"""MATCH (n{cls._label_match_string()})"""

            conditions = []
            params = {}

            # Process equality filters from kwargs
            if equality_filters:
                for field, value in equality_filters.items():
                    param_name = f"{field}_eq"
                    conditions.append(f"n.{field} = ${param_name}")
                    params[param_name] = value

            # Process FilterOp objects
            for i, f in enumerate(filter_ops):
                param_name = f"{f.field}_{i}"
                conditions.append(f"n.{f.field} {f.operator} ${param_name}")
                params[param_name] = f.value

            if conditions:
                query += f"\nWHERE {' AND '.join(conditions)}"

            query += "\nRETURN n"

            log.debug(query)
            log.debug(params)
            nodes = []

            with Base._driver.session() as session:
                result = session.run(query, params)
                for record in result:
                    node = record['n']
                    properties = dict(node.items())

                    # Convert Neo4j types to Python types
                    properties = convert_neo4j_types_to_python(properties)

                    nodes.append(
                        cls(**properties)
                    )

            return nodes

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

    # Create the minimal relationship model class
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

    # Add the model classes to the Base class
    Base.NodeModel = NodeModel
    Base.RelationshipModel = RelationshipModel

    return Base


class Relationship(BaseModel):
    source: str
    rel_type: str
    target: str
    nodes: List[Tuple[Any, Dict]] = []

    # Use PrivateAttr for non-model fields
    _parent_instance: Optional[Any] = PrivateAttr(default=None)

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
        """Helper method to get the Base class"""
        # First try through parent instance if available
        if self._parent_instance is not None:
            for base_class in self._parent_instance.__class__.__mro__:
                if hasattr(base_class, 'get_registry') and hasattr(base_class, 'NodeModel'):
                    return base_class

        # If we can't find it through the parent, use the global registry directly
        from graphio.objects.model import get_global_registry

        # Create a dynamic Base object that uses the global registry
        class DynamicBase:
            @staticmethod
            def get_registry():
                return get_global_registry()

            @staticmethod
            def get_driver():
                # Try to find a driver from loaded modules
                import sys
                for module_name, module in sys.modules.items():
                    if hasattr(module, 'Base') and hasattr(module.Base, 'get_driver'):
                        return module.Base.get_driver()
                # If not found, try to get from test environment
                import inspect
                frame = inspect.currentframe()
                try:
                    while frame:
                        if 'graph' in frame.f_locals:
                            return frame.f_locals['graph']
                        frame = frame.f_back
                finally:
                    del frame

                raise ValueError("Could not find driver")

            @staticmethod
            def get_class_by_name(name):
                registry = DynamicBase.get_registry()
                for cls in registry:
                    if cls.__name__ == name:
                        return cls
                return None

        # Add NodeModel attribute to satisfy checks
        DynamicBase.NodeModel = True
        return DynamicBase

    def relationshipset(self):
        return self.dataset()

    def set(self):
        return self.dataset()

    def match(self):
        # Get the base class to access driver and registry
        base = self._get_base()
        if not base:
            raise ValueError("Could not determine Base class")

        target_node = base.get_class_by_name(self.target)
        driver = base.get_driver()

        query = f"""WITH $properties AS properties
MATCH (source{self._parent_instance._label_match_string()})-[:{self.rel_type}]->(target{target_node._label_match_string()})
WHERE {where_clause_with_properties(self._parent_instance.match_dict, 'properties', node_variable='source')}
RETURN distinct target"""
        log.debug(query)
        instances = []
        with driver.session() as session:
            print(f"\n match parent instance {self._parent_instance.match_dict}")
            result = session.run(query, properties=self._parent_instance.match_dict)

            for record in result:
                node = record['target']
                properties = dict(node.items())
                properties = convert_neo4j_types_to_python(properties)
                instances.append(
                    target_node(**properties)
                )
        return instances

    def delete(self, target=None):
        """
        Delete all relationships of this type between the source and target nodes.
        """
        base = self._get_base()
        if not base:
            raise ValueError("Could not determine Base class")

        target_class = base.get_class_by_name(self.target)
        driver = base.get_driver()

        query = f"""WITH $properties AS properties, $target_properties AS target_properties
        MATCH (source{self._parent_instance._label_match_string()})-[r:{self.rel_type}]->(target{target_class._label_match_string()})
        WHERE {where_clause_with_properties(self._parent_instance.match_dict, 'properties', node_variable='source')} \n"""
        if target:
            query += f" AND {where_clause_with_properties(target.match_dict, 'target_properties', node_variable='target')} \n"
        query += "DELETE r"
        log.debug(query)
        print(f"parent instance {self._parent_instance}")
        print(f"target: {self.target}")

        with driver.session() as session:
            session.run(query, properties=self._parent_instance.match_dict, target_properties=target.match_dict)


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
