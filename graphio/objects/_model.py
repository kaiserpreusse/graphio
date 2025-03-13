from typing import Optional
from typing import List, Union
import logging

from pydantic import BaseModel, PrivateAttr

from graphio import NodeSet, RelationshipSet
from graphio.queries import where_clause_with_properties

log = logging.getLogger(__name__)


class RegistryMeta(type):
    def __init__(cls, name, bases, attrs):
        if not hasattr(cls, '_registry'):
            cls._registry = []
        if cls not in cls._registry.default:
            if cls.__name__ != 'MyBaseModel':
                cls._registry.default.append(cls)

        super().__init__(name, bases, attrs)


class GraphModel(BaseModel):
    _driver = None  # Class variable to store the driver
    _registry: list[type] = PrivateAttr(default=[])

    @classmethod
    def set_driver(cls, driver):
        cls._driver = driver

    @classmethod
    def get_driver(cls):
        if cls._driver is None:
            raise ValueError("Driver is not set. Use set_driver() to set the driver.")
        return cls._driver

    @classmethod
    def get_class_by_name(cls, name):
        for subclass in cls._registry.default:
            if subclass.__name__ == name:
                return subclass
        return None

    @classmethod
    def model_create_index(cls):
        for model in cls._registry.default:
            if issubclass(model, '_NodeModel'):
                model.create_index()


class CustomMeta(RegistryMeta, BaseModel.__class__):
    pass


class _NodeModel(GraphModel, metaclass=CustomMeta):
    _default_props: dict = {}
    _preserve: List[str] = None
    _append_props: List[str] = None
    _additional_labels: List[str] = None

    _labels: List[str] = []
    _merge_keys: List[str] = []

    class Config:
        extra = 'allow'

    def __init__(self, **data):
        super().__init__(**data)
        for k, v in data.items():
            setattr(self, k, v)

        self._validate_merge_keys()

    def _validate_merge_keys(self):
        for key in self._merge_keys:
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
        # get all propertie that are not relationships
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
        labels = cls._labels.default
        merge_keys = cls._merge_keys.default
        default_props = cls._default_props.default
        preserve = cls._preserve.default
        append_props = cls._append_props.default
        additional_labels = cls._additional_labels.default

        return NodeSet(labels=labels, merge_keys=merge_keys, default_props=default_props,
                       preserve=preserve, append_props=append_props, additional_labels=additional_labels)

    @classmethod
    def create_index(cls):
        cls.nodeset().create_index(cls._driver)

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
        return {key: getattr(self, key) for key in self._merge_keys}

    def create_target_nodes(self):
        if self._driver is None:
            raise ValueError("Driver is not set. Use set_driver() to set the driver.")
        for rel in self.relationships:
            if self.__class__.__name__ == rel.source or self.__class__.__name__ == rel.target:
                for other_node, properties in rel.nodes:
                    other_node.create()

    def create_relationships(self) -> None:
        """
        Create relationships for this node.
        """
        if self._driver is None:
            raise ValueError("Driver is not set. Use set_driver() to set the driver.")
        for rel in self.relationships:
            # relationships
            if self.__class__.__name__ == rel.source:
                for other_node, properties in rel.nodes:
                    relset = rel.dataset()
                    relset.add_relationship(self.match_dict, other_node.match_dict, properties)
                    print(relset)
                    relset.create(self._driver)
            elif self.__class__.__name__ == rel.target:
                for other_node, properties in rel.nodes:
                    relset = rel.dataset()
                    relset.add_relationship(other_node.match_dict, self.match_dict, properties)
                    print(relset)
                    relset.create(self._driver)

    def create_node(self):
        if self._driver is None:
            raise ValueError("Driver is not set. Use set_driver() to set the driver.")
        ns = self.nodeset()

        ns.add_node(self._all_properties)
        ns.create(self._driver)

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
        if self._driver is None:
            raise ValueError("Driver is not set. Use set_driver() to set the driver.")
        for rel in self.relationships:
            if self.__class__.__name__ == rel.source or self.__class__.__name__ == rel.target:
                for other_node, properties in rel.nodes:
                    other_node.merge()

    def merge_relationships(self) -> None:
        """
        Merge relationships for this node.
        """
        if self._driver is None:
            raise ValueError("Driver is not set. Use set_driver() to set the driver.")
        for rel in self.relationships:
            # relationships
            if self.__class__.__name__ == rel.source:
                for other_node, properties in rel.nodes:
                    relset = rel.dataset()
                    relset.add_relationship(self.match_dict, other_node.match_dict, properties)
                    relset.merge(self._driver)
            elif self.__class__.__name__ == rel.target:
                for other_node, properties in rel.nodes:
                    relset = rel.dataset()
                    relset.add_relationship(other_node.match_dict, self.match_dict, properties)
                    relset.merge(self._driver)

    def merge_node(self):
        if self._driver is None:
            raise ValueError("Driver is not set. Use set_driver() to set the driver.")
        ns = self.nodeset()

        ns.add_node(self._all_properties)
        ns.merge(self._driver)

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
        return ":" + ":".join(cls._labels.default)

    @classmethod
    def match(cls, **kwargs) -> list[('_NodeModel')]:
        """
        Match and return an instance of this NodeModel.

        :return: NodeModel
        """
        if cls._driver is None:
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

        with cls._driver.session() as session:
            result = session.run(query, properties=kwargs)
            for record in result:
                node = record['n']
                properties = dict(node.items())

                nodes.append(
                    cls(**properties)
                )

        return nodes


class Relationship(BaseModel):
    source: str
    rel_type: str
    target: str
    nodes: List[tuple[_NodeModel, dict]] = []

    def __init__(self, source: str, rel_type: str, target: str, **data):
        super().__init__(source=source, rel_type=rel_type, target=target, **data)

    def add(self, node: _NodeModel, properties: dict = None):
        self.nodes.append((node, properties))

    def dataset(self):
        source_node = _NodeModel.get_class_by_name(self.source)
        target_node = _NodeModel.get_class_by_name(self.target)

        return RelationshipSet(
            rel_type=self.rel_type,
            start_node_labels=source_node._labels.default,
            end_node_labels=target_node._labels.default,
            start_node_properties=source_node._merge_keys.default,
            end_node_properties=target_node._merge_keys.default,
        )

    def relationshipset(self):
        return self.dataset()

    def set(self):
        return self.dataset()


class Graph(BaseModel):

    def create(self, *objects: _NodeModel):
        for o in objects:
            if isinstance(o, _NodeModel):
                o.create_node()
        for o in objects:
            if isinstance(o, _NodeModel):
                o.create_relationships()

    def merge(self, *objects: _NodeModel):
        for o in objects:
            if isinstance(o, _NodeModel):
                o.merge_node(self.driver)
        for o in objects:
            if isinstance(o, _NodeModel):
                o.merge_relationships(self.driver)
