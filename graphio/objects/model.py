from typing import List, Union
import importlib
import pkgutil
import logging
from neo4j import Driver

from graphio import NodeSet, RelationshipSet
from graphio.queries import where_clause_with_properties

log = logging.getLogger(__name__)


class RegistryMeta(type):
    def __init__(cls, name, bases, attrs):
        if not hasattr(cls, 'registry'):
            cls.registry = []
        if cls not in cls.registry:
            if cls.__name__ != 'NodeModel' and cls.__name__ != 'RelationshipModel' and cls.__name__ != 'ModelBase':
                cls.registry.append(cls)
        super().__init__(name, bases, attrs)


class ModelBase(metaclass=RegistryMeta):

    @classmethod
    def get_class_by_name(cls, name):
        for subclass in cls.registry:
            if subclass.__name__ == name:
                return subclass
        return None


class NodeModel(ModelBase):
    """
    Entrypoint for the application.
    """
    labels: List[str]
    merge_keys: List[str]
    default_props: dict = None
    preserve: List[str] = None
    append_props: List[str] = None
    additional_labels: List[str] = None

    def __init__(self, properties):
        self.properties = properties
        if not all([key in properties for key in self.merge_keys]):
            raise ValueError(f"Missing merge key properties: {self.merge_keys}")

    @property
    def relationships(self):
        # get all attributes of this class that are instances of Relationship
        relationship_objects = []
        for attr_name in dir(self):
            if attr_name != 'relationships':
                attr = getattr(self, attr_name)
                if isinstance(attr, Relationship):
                    relationship_objects.append(attr)
        return relationship_objects

    @property
    def match_dict(self):
        return {key: self.properties[key] for key in self.merge_keys}

    def create_node(self, driver):
        # this node
        ns = self.nodeset()
        ns.add_node(self.properties)
        ns.create(driver)

    def create_relationships(self, driver):
        # nodes from relationships
        for rel in self.relationships:
            # relationships
            if self.__class__.__name__ == rel.source:
                for other_node, properties in rel.nodes:
                    relset = rel.dataset()
                    relset.add_relationship(self.match_dict, other_node.match_dict, properties)
                    relset.create(driver)
            elif self.__class__.__name__ == rel.target:
                for other_node, properties in rel.nodes:
                    relset = rel.dataset()
                    relset.add_relationship(other_node.match_dict, self.match_dict, properties)
                    relset.create(driver)

    def merge_node(self, driver):
        # this node
        ns = self.nodeset()
        ns.add_node(self.properties)
        ns.merge(driver)

    def merge_relationships(self, driver):
        # nodes from relationships
        for rel in self.relationships:
            # relationships
            if self.__class__.__name__ == rel.source:
                for other_node, properties in rel.nodes:
                    relset = rel.dataset()
                    relset.add_relationship(self.match_dict, other_node.match_dict, properties)
                    relset.merge(driver)
            elif self.__class__.__name__ == rel.target:
                for other_node, properties in rel.nodes:
                    relset = rel.dataset()
                    relset.add_relationship(other_node.match_dict, self.match_dict, properties)
                    relset.merge(driver)

    @classmethod
    def _label_match_string(cls):
        return ":" + ":".join(cls.labels)

    @classmethod
    def match(cls, properties: dict, driver: Driver) -> list[('NodeModel')]:
        """
        Match and return an instance of this NodeModel.

        :return: NodeModel
        """

        query = f"""WITH $properties AS properties
        MATCH (n{cls._label_match_string()})
        WHERE {where_clause_with_properties(properties, 'properties', node_variable='n')}
        RETURN n"""
        log.debug(query)
        nodes = []

        with driver.session() as session:
            result = session.run(query, properties=properties)
            for record in result:
                node = record['n']
                properties = dict(node.items())

                nodes.append(
                    cls(properties)
                )

        return nodes

    @classmethod
    def nodeset(cls):
        """
        Create a NodeSet from this Node.

        :return: NodeSet
        """
        labels = cls.labels
        merge_keys = cls.merge_keys
        default_props = cls.default_props
        preserve = cls.preserve
        append_props = cls.append_props
        additional_labels = cls.additional_labels

        return NodeSet(labels=labels, merge_keys=merge_keys, default_props=default_props,
                       preserve=preserve, append_props=append_props, additional_labels=additional_labels)

    @classmethod
    def dataset(cls):
        return cls.nodeset()

    @classmethod
    def create_index(cls, driver):
        cls.nodeset().create_index(driver)


class Relationship:

    def __init__(self, source: str, rel_type: str, target: str):
        self.rel_type = rel_type
        self.source = source
        self.target = target
        self.nodes = []

    def add(self, node: NodeModel, properties: dict = None):
        self.nodes.append((node, properties))

    def dataset(self):
        source_node = NodeModel.get_class_by_name(self.source)
        target_node = NodeModel.get_class_by_name(self.target)

        return RelationshipSet(
            rel_type=self.rel_type,
            start_node_labels=source_node.labels,
            end_node_labels=target_node.labels,
            start_node_properties=source_node.merge_keys,
            end_node_properties=target_node.merge_keys
        )

    def relationshipset(self):
        return self.dataset()

    def set(self):
        return self.dataset()


class RelationshipModel(ModelBase):
    rel_type: str
    source: type[NodeModel]
    target: type[NodeModel]
    default_props: dict = None

    @classmethod
    def relationshipset(cls):
        return RelationshipSet(
            rel_type=cls.rel_type,
            start_node_labels=cls.source.labels,
            end_node_labels=cls.target.labels,
            start_node_properties=cls.source.merge_keys,
            end_node_properties=cls.target.merge_keys,
            default_props=cls.default_props
        )

    @classmethod
    def dataset(self):
        return self.relationshipset()

    @classmethod
    def create_index(cls, driver):
        cls.relationshipset().create_index(driver)


class Graph:

    def __init__(self, driver):
        self.driver = driver

    def create(self, *objects: NodeModel):
        for o in objects:
            if isinstance(o, NodeModel):
                o.create_node(self.driver)
        for o in objects:
            if isinstance(o, NodeModel):
                o.create_relationships(self.driver)

    def merge(self, *objects: Union[NodeModel, RelationshipModel]):
        for o in objects:
            if isinstance(o, NodeModel):
                o.merge_node(self.driver)
        for o in objects:
            if isinstance(o, NodeModel):
                o.merge_relationships(self.driver)

    def create_index(self, model: Union[NodeModel, RelationshipModel]):
        model.create_index(self.driver)


def model_initialize(module_name):
    package = importlib.import_module(module_name)
    if hasattr(package, '__path__'):
        for _, mod_name, _ in pkgutil.iter_modules(package.__path__):
            importlib.import_module(f"{module_name}.{mod_name}")
    else:
        importlib.import_module(module_name)


def model_create_index(driver):
    for model in ModelBase.registry:
        if issubclass(model, NodeModel):
            model.create_index(driver)
