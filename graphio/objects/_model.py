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
            print(f"Setting {k} to {v}")
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

    def create(self):
        if self._driver is None:
            raise ValueError("Driver is not set. Use set_driver() to set the driver.")
        ns = self.nodeset()

        properties = {}
        for field in self.model_fields:
            properties[field] = getattr(self, field)

        properties.update(self._additional_properties)

        ns.add_node(properties)
        ns.create(self._driver)

    def merge(self):
        if self._driver is None:
            raise ValueError("Driver is not set. Use set_driver() to set the driver.")
        ns = self.nodeset()

        properties = {}
        for field in self.model_fields:
            properties[field] = getattr(self, field)

        properties.update(self._additional_properties)

        ns.add_node(properties)
        ns.merge(self._driver)

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
    nodes: List[Union[_NodeModel]] = []

    def __init__(self, source: str, rel_type: str, target: str, **data):
        super().__init__(source=source, rel_type=rel_type, target=target, **data)

    def add(self, node: _NodeModel):
        self.nodes.append(node)

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
