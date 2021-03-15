from datetime import datetime
from typing import Iterable, List, Optional, TypeVar

from py2neo import Node, Record
from py2neo.cypher import cypher_escape, cypher_repr

from cups.db import graph

__all__ = [
    'encode_dict',
    'encode_filter',
    'expire',
    'set_expire',
    'get_one',
    'ForeignKey',
    'Model',
    'NodeType',
]


def encode_dict(obj: dict) -> str:
    for key, value in obj.items():
        if not isinstance(key, str):
            raise ValueError('Key must be str')
        elif not isinstance(value, (int, float, bool, str)) and value is not None:
            raise ValueError('Value must be number, bool, str or null')
    return cypher_repr(obj)


def encode_filter(id: int = None, /, **kwargs) -> str:
    f = []
    if id is not None:
        if not isinstance(id, int):
            raise ValueError('Model ID must be int')
        f.append(f'id(i) = {id}')
    if kwargs:
        for key, value in kwargs.items():
            if not isinstance(value, (int, float, bool, str)) and value is not None:
                raise ValueError('Invalid property data type')
            try:
                f.append(f'i.{cypher_escape(key)} = {cypher_repr(value)}')
            except ValueError:
                raise ValueError('Invalid property data')
    return ' AND '.join(f)


def expire(dt: datetime = None) -> str:
    return cypher_repr({'expire_at': dt.timestamp()}) if dt else ''


def set_expire(i: str, dt: datetime = None) -> str:
    return f'SET {i}.expire_at = {cypher_repr(dt.timestamp())} ' if dt else ' '


def get_one(cursor) -> Optional[Record]:
    try:
        return next(cursor)
    except StopIteration:
        pass
    finally:
        cursor.close()


class _ModelType(type):
    @property
    def label(cls) -> str:
        return ':'.join([i.__name__ for i in cls.__mro__[:-3] if not i.__name__.startswith('_')])


def foreign_key(model_name: str, relationship: str):
    def _get(self) -> Optional['NodeType']:
        model = Model.get_model(model_name)
        cursor = graph.run(
            f'MATCH (i:{self.label}) -[:{relationship}]-> (j:{model.label}) '
            f'WHERE id(i) = {self.id} RETURN j'
        )
        if record := get_one(cursor):
            return model.from_node(record['j'])

    def _del(self) -> None:
        model = Model.get_model(model_name)
        graph.run(
            f'MATCH (i:{self.label}) -[r:{relationship}]-> (:{model.label}) '
            f'WHERE id(i) = {self.id} DELETE r'
        )

    def _set(self, item: 'NodeType') -> None:
        _del(self)
        model = Model.get_model(model_name)
        graph.run(
            f'MATCH (i:{self.label}) WHERE id(i) = {self.id} '
            f'MATCH (j:{model.label}) WHERE id(j) = {item.id} '
            f'MERGE (i) -[:{relationship}]-> (j)'
        )

    return property(_get, _set, _del)


ForeignKey = foreign_key


class Model(dict, metaclass=_ModelType):
    __slots__ = ('id',)
    __registry__ = {}

    def __init__(self, id: int = None, **kwargs):
        self.id = id
        super().__init__(kwargs)

    @property
    def label(self) -> str:
        return ':'.join([i.__name__ for i in self.__class__.__mro__[:-3] if not i.__name__.startswith('_')])

    @classmethod
    def get_one(cls, id: int = None, /, **kwargs) -> Optional['NodeType']:
        cursor = graph.run(f'MATCH (i:{cls.label}) WHERE {encode_filter(id, **kwargs)} RETURN i')
        record = get_one(cursor)
        if record:
            return cls.from_node(record['i'])

    @classmethod
    def get_or_create(cls, default: dict = None, expire_at: datetime = None, **kwargs) -> ('NodeType', bool):
        """
        :param default: - initial params for newly created Nodes
        :param kwargs: - search params, also used in Node creation. may be overwritten by default
        :return: tuple[Node, created]
        """
        record = get_one(graph.run(f'MATCH (i:{cls.label} {encode_dict(kwargs)}) RETURN i'))
        if record:
            return cls.from_node(record['i']), False
        else:
            return cls.create(**{**kwargs, **(default or {})}), True

    @classmethod
    def get_all(cls, id: int = None, /, **kwargs) -> Iterable['NodeType']:
        cursor = graph.run(f'MATCH (i:{cls.label}) WHERE {encode_filter(id, **kwargs)} RETURN i')
        for record in cursor:
            yield cls.from_node(record['i'])

    @classmethod
    def from_node(cls, node: Node) -> 'NodeType':
        if cls == Model:
            for label in node.labels:
                if model := Model.get_model(label):
                    return model(node.identity, **node)
        else:
            return cls(node.identity, **node)

    def as_node(self):
        node = Node(self.label, **self)
        node.identity = self.id
        return node

    @classmethod
    def create(cls, **kwargs):
        instance = cls(**kwargs)
        instance.save()
        return instance

    def save(self, update_fields: List[str] = None):
        if self.id:
            fields = set(self.keys())
            if update_fields:
                fields &= set(update_fields)
            data = ', '.join([
                f'i.{cypher_escape(field)} = {cypher_repr(self[field])}'
                for field in fields
            ])
            graph.run(f'MATCH (i:{self.label}) WHERE id(i) = {self.id} SET {data} RETURN i')
        else:
            self.id = next(graph.run(f'CREATE (i:{self.label} {encode_dict(self)}) RETURN id(i) as i'))['i']

    def delete(self):
        if self.id:
            graph.run(f'MATCH (i:{self.label}) WHERE id(i) = {self.id} DETACH DELETE i')
            self.id = None

    def __init_subclass__(cls, *args, **kwargs) -> None:
        Model.__registry__[cls.__name__] = cls
        super().__init_subclass__(*args, **kwargs)

    @classmethod
    def get_model(cls, model_name: str):
        return cls.__registry__.get(model_name)

    def as_dict(self, update: dict = None):
        d = {**self, 'id': self.id}
        if update:
            d.update(update)
        return d

    def __hash__(self):
        return hash(f'{self.label}:{self.id}')

    def __str__(self) -> str:
        return f'[{self.label} {self.id}] {cypher_repr(self)}'


NodeType = TypeVar('NodeType', bound=Model)
