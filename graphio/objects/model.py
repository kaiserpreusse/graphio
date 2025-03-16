import importlib
import inspect
import sys
import pkgutil
import logging
from typing import List, ClassVar, Dict, Any, Optional, Type, Union, Set, Tuple

from pydantic import BaseModel, PrivateAttr

from graphio import NodeSet, RelationshipSet
from graphio.queries import where_clause_with_properties

log = logging.getLogger(__name__)

_GLOBAL_REGISTRY = None

def get_global_registry():
    global _GLOBAL_REGISTRY
    if _GLOBAL_REGISTRY is None:
        _GLOBAL_REGISTRY = Registry()
    return _GLOBAL_REGISTRY


class Registry:
    def __init__(self):
        self.default = []
        self._is_initialized = False

    def __iter__(self):
        return iter(self.default)

    def add(self, cls):
        # Check if the class is already in the registry by class object (not just name)
        if cls not in self.default:
            print(f"Adding {cls.__name__} to registry")
            self.default.append(cls)
            return True
        return False

    def auto_discover(self):
        """Auto-discover all model classes in the caller's module and related modules"""
        if self._is_initialized:
            return
        print("doing auto discover")

        # Get the frame that called this method
        frame = inspect.currentframe().f_back.f_back
        module = inspect.getmodule(frame)

        if not module:
            return

        module_name = module.__name__
        print(f"Auto-discovering in module: {module_name}")

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
        print(f"Scanning module: {module.__name__}")

        # Find and register all classes with _labels and _merge_keys
        for name, obj in inspect.getmembers(module):
            if inspect.isclass(obj) and hasattr(obj, '_labels') and hasattr(obj, '_merge_keys'):
                print(f"Found model class: {obj.__name__}")
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
                    print(f"Added {obj.__name__} to registry using Base class")
                else:
                    # Fall back to adding directly to this registry
                    self.add(obj)
                    print(f"Added {obj.__name__} to registry directly")


class CustomMeta(BaseModel.__class__):
    def __init__(cls, name, bases, attrs):
        super().__init__(name, bases, attrs)

        # Skip base classes
        if name not in ('Base', 'NodeModel', 'RelationshipModel'):
            # Use Base's registry if it exists
            if hasattr(cls, 'get_registry'):
                registry = cls.get_registry()
                registry.add(cls)


def declarative_base():
    """
    Create a declarative base for Neo4j model definitions.
    Similar to SQLAlchemy's declarative_base but works with Pydantic.
    """
    # Create the base class
    class Base(BaseModel, metaclass=CustomMeta):
        _driver = None

        @classmethod
        def initialize(cls):
            """Initialize the Base class"""
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
    class NodeModel(BaseModel, metaclass=CustomMeta):
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
            self._initialize_relationships()
            for k, v in data.items():
                setattr(self, k, v)

            self._validate_merge_keys()

        def _validate_merge_keys(self):
            for key in self.__class__._merge_keys:
                if key not in self.model_fields:
                    raise ValueError(f"Merge key '{key}' is not a valid model field.")

        def _initialize_relationships(self):
            """Initialize all relationship attributes defined on the class"""
            # Get all fields from model_fields
            for field_name, field_info in self.model_fields.items():
                # Check if the field's default value is a Relationship
                field_default = field_info.default

                if isinstance(field_default, Relationship):
                    # Create a new relationship instance with this object as parent
                    relationship = Relationship(
                        source=field_default.source,
                        rel_type=field_default.rel_type,
                        target=field_default.target,
                        parent=self
                    )
                    # Directly set the attribute on self, overriding the class attribute
                    setattr(self, field_name, relationship)

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
            return {key: getattr(self, key) for key in self.__class__._merge_keys}

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
                    for other_node, properties in rel.nodes:
                        relset = rel.dataset()
                        relset.add_relationship(self.match_dict, other_node.match_dict, properties)
                        relset.merge(Base._driver)
                elif self.__class__.__name__ == rel.target:
                    for other_node, properties in rel.nodes:
                        relset = rel.dataset()
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
        def match(cls, **kwargs) -> List['NodeModel']:
            """
            Match and return an instance of this NodeModel.

            :return: NodeModel
            """
            if Base._driver is None:
                raise ValueError("Driver is not set. Use set_driver() to set the driver.")

            # check if kwargs are valid model fields
            for key in kwargs.keys():
                if key not in cls.model_fields:
                    log.warning(f"Key '{key}' is not a valid model field. We try to match but the result will not"
                                f"be accessible as a model instance attribute.")

            query = f"""WITH $properties AS properties
            MATCH (n{cls._label_match_string()})
            WHERE {where_clause_with_properties(kwargs, 'properties', node_variable='n')}
            RETURN n"""
            log.debug(query)
            nodes = []

            with Base._driver.session() as session:
                result = session.run(query, properties=kwargs)
                for record in result:
                    node = record['n']
                    properties = dict(node.items())

                    nodes.append(
                        cls(**properties)
                    )

            return nodes

    # Create the minimal relationship model class
    class RelationshipModel(BaseModel, metaclass=CustomMeta):
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

    def add(self, node: Any, properties: Dict = None):
        """Add a target node to this relationship"""
        self.nodes.append((node, properties or {}))
        return self  # Allow method chaining

    def dataset(self):

        # Add debugging to see what's in the registry
        base = self._get_base()
        registry = base.get_registry()
        print(f"Registry contains {len(list(registry))} classes:")
        for cls in registry:
            print(f"  - {cls.__name__}")

        print("base")
        print(base)
        print(self.source)
        print(self.target)
        print(base.get_class_by_name(self.source))

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
        # Since we're using a global registry, we can use any reference to Base
        import sys
        for module_name in sys.modules:
            module = sys.modules[module_name]
            if hasattr(module, 'Base'):
                base = getattr(module, 'Base')
                if hasattr(base, 'get_registry'):
                    return base

        return None

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

        instances = []
        with driver.session() as session:
            result = session.run(query, properties=self._parent_instance.match_dict)

            for record in result:
                node = record['target']
                properties = dict(node.items())
                instances.append(
                    target_node(**properties)
                )
        return instances


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