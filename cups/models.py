from collections import namedtuple
from datetime import datetime
from typing import Iterable, List, Optional

from py2neo import cypher_repr

from cups.db import graph
from cups.utils import *

__all__ = [
    'Entity',
    'Group',
    'Perm',
    'Scope',
    'Ability',
    'EnabledAbility',
    'IS_IN',
    'IS_IN_AUTO',
    'ALLOW',
    'DENY',
    'INHERITS',
    'EXISTS_IN',
    'SUBSET_OF',
    'SUPPORTS',
    'ACTIVATED',
    'ENABLED',
    'RELATED_TO',
    'WORKS_IN',
]

IS_IN = 'IS_IN'
IS_IN_AUTO = 'IS_IN_AUTO'
ALLOW = 'ALLOW'
DENY = 'DENY'
INHERITS = 'INHERITS'
EXISTS_IN = 'EXISTS_IN'
SUBSET_OF = 'SUBSET_OF'
SUPPORTS = 'SUPPORTS'
ACTIVATED = 'ACTIVATED'
ENABLED = 'ENABLED'
RELATED_TO = 'RELATED_TO'
WORKS_IN = 'ACTIVATED_IN'


class _HasScope(Model):
    @property
    def scope(self) -> Optional['NodeType']:
        cursor = graph.run(f'MATCH (i:{self.label}) -[:{EXISTS_IN}]-> (j:{Scope.label}) WHERE id(i)={self.id} RETURN j')
        if record := get_one(cursor):
            return Scope.from_node(record['j'])

    @scope.deleter
    def scope(self) -> None:
        self['__scope_id__'] = None
        graph.run(f'MATCH (i:{self.label}) -[r:{EXISTS_IN}]-> (:{Scope.label}) WHERE id(i)={self.id} '
                  f'SET i.__scope_id__ = {cypher_repr(None)} DELETE r')

    @scope.setter
    def scope(self, item: 'NodeType') -> None:
        graph.run(f'MATCH (i:{self.label}) -[r:{EXISTS_IN}]-> (:{Scope.label}) WHERE id(i)={self.id} DELETE r')
        self['__scope_id__'] = item.id
        graph.run(
            f'MATCH (i:{self.label}) WHERE id(i) = {self.id} '
            f'MATCH (j:{Scope.label}) WHERE id(j) = {item.id} '
            f'MERGE (i) -[:{EXISTS_IN}]-> (j) SET i.__scope_id__ = {cypher_repr(item.id)}'
        )

    def is_scope_supported(self, scope: 'Scope' = None) -> None:
        local = self.scope
        if not local or (scope and local.id == scope.id):
            return
        if not scope:
            raise ValueError(f'{self.label} only works in scope {local}')
        cursor = graph.run(
            f'MATCH (a:{self.label}) WHERE id(a) = {self.id} '
            f'MATCH (a)-[:{EXISTS_IN}|{SUBSET_OF}*]->(:{Scope.label})<-[:{SUBSET_OF}]-(s:{Scope.label}) '
            f'WHERE id(s) = {scope.id} RETURN id(s) as i')
        if not get_one(cursor):
            raise ValueError(f'{self.label} only works in scope {local}')


class Entity(Model):
    def get_groups(self, scope: 'Scope' = None) -> Iterable['Group']:
        if scope:
            cursor = graph.run(
                f'MATCH (s:{Scope.label}) WHERE id(s) = {scope.id} '
                f'MATCH (e:{self.label}) -[:{IS_IN}]-> (g:{Group.label}) -[:{EXISTS_IN}|{SUBSET_OF}*]-> (s) '
                f'WHERE NOT (e)-[:{IS_IN_AUTO}]-> (g:{Group.label}) AND id(e) = {self.id} '
                f'RETURN g'
            )
        else:
            cursor = graph.run(
                f'MATCH (e:{self.label}) -[:{IS_IN}]-> (g:{Group.label}) '
                f'WHERE NOT (e)-[:{IS_IN_AUTO}]-> (g:{Group.label}) AND id(e) = {self.id} '
                f'RETURN g'
            )
        for record in cursor:
            yield Group.from_node(record['g'])
        yield Group.get_global()

    def add_to_group(self, group: 'Group', scope: 'Scope' = None, expire_at: datetime = None):
        group.is_scope_supported(scope)

        f = {'scope_id': scope.id if scope else '*'}
        graph.run(f'MATCH (e:{self.label}) WHERE id(e) = {self.id} '
                  f'MATCH (g:{Group.label}) WHERE id(g) = {group.id} '
                  f'MERGE (e)-[r:{IS_IN} {f}]->(g) '
                  f'{set_expire("r", expire_at)}')

    def remove_from_group(self, group: 'Group', scope: 'Scope' = None):
        f = {'scope_id': scope.id if scope else '*'}
        graph.run(f'MATCH (e:{self.label})-[r:{IS_IN} {encode_dict(f)}]->(g:{Group.label}) '
                  f'WHERE id(e) = {self.id} and id(g) = {group.id} DELETE r')

    def remove_from_all_groups_in_scope(self, scope: 'Scope' = None):
        f = {'scope_id': scope.id if scope else '*'}
        graph.run(f'MATCH (e:{self.label})-[r:{IS_IN} {encode_dict(f)}]->(g:{Group.label}) '
                  f'WHERE id(e) = {self.id} DELETE r')

    def remove_from_all_groups(self):
        graph.run(f'MATCH (e:{self.label})-[r:{IS_IN}]->(g:{Group.label}) '
                  f'WHERE id(e) = {self.id} DELETE r')

    def get_all_activated_abilities(self) -> Iterable['EnabledAbility']:
        cursor = graph.run(f'MATCH (e:{self.label}) WHERE id(e) = {self.id} '
                           f'MATCH (e)-[r:{ENABLED}]->(a:{Ability.label}) '
                           f'RETURN r, a')
        for record in cursor:
            ability = Ability.from_node(record['a'])
            edge = record['r']
            yield EnabledAbility(ability=ability, perm_id=edge['perm_id'], scope_id=edge['scope_id'])

    def get_activated_abilities(self, scope: 'Scope' = None) -> Iterable['Ability']:
        f = encode_dict({'scope_id': scope.id})
        cursor = graph.run(f'MATCH (e:{self.label}) WHERE id(e) = {self.id} '
                           f'MATCH (e)-[r:{ENABLED} {f}]->(a:{Ability.label}) '
                           f'RETURN r, a')
        for record in cursor:
            ability = Ability.from_node(record['a'])
            edge = record['r']
            yield EnabledAbility(ability=ability, perm_id=edge['perm_id'], scope_id=edge['scope_id'])

    def activate_ability(self, ability: 'Ability', perm: 'Perm', scope: 'Scope' = None, expire_at: datetime = None):
        ability.is_scope_supported(scope)

        if not ability.is_perm_supported(perm):
            raise ValueError('Permission is not supported by this ability')

        graph.run(f'MATCH (e:{self.label}) WHERE id(e)={self.id} '
                  f'MATCH (a:{ability.label}) WHERE id(a)={ability.id} '
                  f'MERGE (e)-[r:{ENABLED} {{perm_id: {perm.id}}}]->(a) '
                  f'{set_expire("r", expire_at)}'
                  f'SET r.scope_id = {cypher_repr(scope.id if scope else "*")}')

    def reset_ability(self, ability: 'Ability', scope: 'Scope' = None):
        scope_id = cypher_repr(scope.id if scope else '*')
        graph.run(f'MATCH (e:{self.label})-[r:{ENABLED}]->(a:{ability.label}) '
                  f'WHERE id(e) = {self.id} AND id(a) = {ability.id} AND r.scope_id = {scope_id} '
                  f'DELETE r')

    def reset_ability_in_all_scopes(self, ability: 'Ability'):
        graph.run(f'MATCH (e:{self.label})-[r:{ENABLED}]->(a:{ability.label}) '
                  f'WHERE id(e) = {self.id} AND id(a) = {ability.id} '
                  f'DELETE r')

    def reset_all_abilities(self):
        graph.run(f'MATCH (e:{self.label})-[r:{ENABLED}]->(a:{Ability.label}) '
                  f'WHERE id(e) = {self.id} DELETE r')

    def save(self, update_fields: List[str] = None):
        super().save(update_fields=update_fields)
        graph.run(f'MATCH (e:{self.label})-[r:{IS_IN_AUTO}]->(:{Group.label}) WHERE id(e) = {self.id} DELETE r')
        graph.run(
            f'MATCH (e:{self.label}) WHERE id(e) = {self.id} '
            f'MATCH (g:{Group.label} {encode_dict({"__global__": True})}) '
            f'MERGE (e)-[r:{IS_IN_AUTO}]->(g)')

    def get_linked_perms(self, scope: 'Scope' = None) -> (Iterable['Perm'], bool):
        f = {'scope_id': scope.id if scope else '*'}
        cursor = graph.run(
            f'MATCH (e:{self.label}) -[r:{ALLOW}|{DENY} {encode_dict(f)}]-> (p:{Perm.label}) '
            f'WHERE id(e) = {self.id} RETURN p, type(r) as r')
        for record in cursor:
            yield Perm.from_node(record['p']), record['r'] == ALLOW

    def get_all_linked_perms(self) -> (Iterable['Perm'], bool):
        cursor = graph.run(
            f'MATCH (e:{self.label}) -[r:{ALLOW}|{DENY}]-> (p:{Perm.label}) '
            f'WHERE id(e) = {self.id} RETURN p, type(r) as r')
        for record in cursor:
            yield Perm.from_node(record['p']), record['r'] == ALLOW

    def link_perm(self, perm: 'Perm', /, scope: 'Scope' = None, allow: bool = True, expire_at: datetime = None):
        perm.is_scope_supported(scope)
        self.reset_perm(perm, scope=scope)
        f = {'scope_id': scope.id if scope else '*'}
        graph.run(f'MATCH (e:{self.label}) WHERE id(e) = {self.id} '
                  f'MATCH (p:{Perm.label}) WHERE id(p) = {perm.id} '
                  f'MERGE (e)-[r:{ALLOW if allow else DENY} {encode_dict(f)}]->(p) '
                  f'{set_expire("r", expire_at)}')

    def reset_perm(self, perm: 'Perm', scope: 'Scope' = None):
        f = {'scope_id': scope.id if scope else '*'}
        graph.run(f'MATCH (e:{self.label})-[r:{ALLOW}|{DENY} {encode_dict(f)}]->(p:{Perm.label}) '
                  f'WHERE id(e) = {self.id} AND id(p) = {perm.id} DELETE r')

    def reset_all_perms_in_scope(self, scope: 'Scope' = None):
        f = {'scope_id': scope.id if scope else '*'}
        graph.run(f'MATCH (e:{self.label})-[r:{ALLOW}|{DENY} {encode_dict(f)}]->(p:{Perm.label}) '
                  f'WHERE id(e) = {self.id} DELETE r')

    def reset_all_perms(self):
        graph.run(f'MATCH (e:{self.label})-[r:{ALLOW}|{DENY}]->(p:{Perm.label}) '
                  f'WHERE id(e) = {self.id} DELETE r')

    def get_allowed_perms(self, scope: 'Scope' = None) -> Iterable['Perm']:
        if scope:
            scope_ids = cypher_repr([i['i'] for i in graph.run(
                f'MATCH (s:{Scope.label})-[:{SUBSET_OF}*]->(ss:{Scope.label}) '
                f'WHERE id(s) = {scope.id} RETURN id(ss) as i'
            )] + [scope.id, '*'])
            cursor = graph.run(f"""
                MATCH (s:{Scope.label}) WHERE id(s) IN {scope_ids}
                CALL {{
                    MATCH (e:{self.label}) WHERE id(e) = {self.id} RETURN e
                    UNION
                    WITH s RETURN s as e
                }} 
                MATCH r = shortestPath((e)-[*1..16]->(p:{Perm.label}))
                WITH relationships(r) as r, tail(reverse(tail(reverse(nodes(r))))) as n, p
                WHERE type(r[-1]) = "ALLOW"
                    AND (r[-1].scope_id IN {scope_ids} OR NOT EXISTS(r[-1].scope_id))
                    AND all(i IN n WHERE i.__scope_id__ IN {scope_ids} OR NOT EXISTS(i.__scope_id__))
                RETURN p""")
        else:
            cursor = graph.run(
                f'MATCH (e:{self.label}) WHERE id(e) = {self.id} '
                f'MATCH r = shortestPath((e)-[*1..16]->(p:{Perm.label})) '
                f'WITH type(relationships(r)[-1]) = "{ALLOW}" as k, p '
                f'WHERE k RETURN p')
        for record in cursor:
            yield Perm.from_node(record['p'])

    def is_able(self, perm: 'Perm', scope: 'Scope' = None) -> bool:
        if scope:
            scope_ids = cypher_repr([i['i'] for i in graph.run(
                f'MATCH (s:{Scope.label})-[:{SUBSET_OF}*]->(ss:{Scope.label}) '
                f'WHERE id(s) = {scope.id} RETURN id(ss) as i'
            )] + [scope.id, '*'])
            cursor = graph.run(f"""
                MATCH (s:{Scope.label}) WHERE id(s) IN {scope_ids}
                CALL {{
                    MATCH (e:{self.label}) WHERE id(e) = {self.id} RETURN e
                    UNION
                    WITH s RETURN s as e
                }} 
                MATCH (p:{Perm.label}) WHERE id(p) = {perm.id}
                MATCH r = shortestPath((e)-[*1..16]->(p))
                WITH relationships(r) as r, tail(reverse(tail(reverse(nodes(r))))) as n, p
                WHERE type(r[-1]) = "ALLOW"
                    AND (r[-1].scope_id IN {scope_ids} OR NOT EXISTS(r[-1].scope_id))
                    AND all(i IN n WHERE i.__scope_id__ IN {scope_ids} OR NOT EXISTS(i.__scope_id__))
                RETURN id(p) as p""")
        else:
            cursor = graph.run(
                f'MATCH (e:{self.label}) WHERE id(e) = {self.id} '
                f'MATCH (p:{Perm.label}) WHERE id(p) = {perm.id} '
                f'MATCH r = shortestPath((e)-[*1..16]->(p)) '
                f'WITH type(relationships(r)[-1]) = "{ALLOW}" as k, p '
                f'WHERE k RETURN id(p) as p')
        return get_one(cursor) is not None


class Group(_HasScope, Model):
    inherits = ForeignKey('Group', INHERITS)  # type: Optional['Group']

    @classmethod
    def get_global(cls):
        group = cls.get_one(__global__=True)
        if not group:
            group = cls.create(name='*')
            group.make_global(force=True)
        return group

    def make_global(self, force: bool = False):
        if self.get('__global__') is True:
            return
        global_group = self.get_one(__global__=True)
        if global_group:
            if global_group.id == self.id:
                return
            if not force:
                raise RuntimeError(f'Can not make group {self} global: {global_group} exists')
            global_group.make_optional()
        self['__global__'] = True
        graph.run(
            f'MATCH (g:{self.label}) WHERE id(g) = {self.id} '
            f'MATCH (e:{Entity.label}) '
            f'MERGE (e)-[:{IS_IN_AUTO}]->(g) '
            f'SET g.__global__ = true')

    def make_optional(self):
        if self.get('__global__') is not True:
            return
        self.pop('__global__')
        graph.run(f'MATCH (:{Entity.label})-[r:{IS_IN_AUTO}]->(g:{self.label} '
                  f'WHERE id(g) = {self.id} '
                  f'SET g.__global__ = null '
                  f'DELETE r')

    def get_linked_perms(self) -> (Iterable['Perm'], bool):
        cursor = graph.run(
            f'MATCH (e:{self.label}) -[r:{ALLOW}|{DENY}]-> (p:{Perm.label}) '
            f'WHERE id(e) = {self.id} RETURN p, type(r) as r')
        for record in cursor:
            yield Perm.from_node(record['p']), record['r'] == ALLOW

    def link_perm(self, perm: 'Perm', /, allow: bool = True, expire_at: datetime = None):
        self.reset_perm(perm)
        graph.run(f'MATCH (e:{self.label}) WHERE id(e) = {self.id} '
                  f'MATCH (p:{Perm.label}) WHERE id(p) = {perm.id} '
                  f'MERGE (e)-[r:{ALLOW if allow else DENY}]->(p) '
                  f'{set_expire("r", expire_at)}')

    def link_all_perms(self, /, allow: bool = True, expire_at: datetime = None):
        self.reset_all_perms()
        graph.run(f'MATCH (e:{self.label}) WHERE id(e) = {self.id} '
                  f'MATCH (p:{Perm.label}) '
                  f'MERGE (e)-[r:{ALLOW if allow else DENY}]->(p) '
                  f'{set_expire("r", expire_at)}')

    def reset_perm(self, perm: 'Perm'):
        graph.run(f'MATCH (e:{self.label})-[r:{ALLOW}|{DENY}]->(p:{Perm.label}) '
                  f'WHERE id(e) = {self.id} AND id(p) = {perm.id} DELETE r')

    def reset_all_perms(self):
        graph.run(f'MATCH (e:{self.label})-[r:{ALLOW}|{DENY}]->(p:{Perm.label}) '
                  f'WHERE id(e) = {self.id} DELETE r')

    def get_allowed_perms(self, scope: 'Scope' = None) -> Iterable['Perm']:
        if scope:
            scope_ids = cypher_repr([i['i'] for i in graph.run(
                f'MATCH (s:{Scope.label})-[:{SUBSET_OF}*]->(ss:{Scope.label}) '
                f'WHERE id(s) = {scope.id} RETURN id(ss) as i'
            )] + [scope.id, '*'])
            cursor = graph.run(f"""
                MATCH (s:{Scope.label}) WHERE id(s) IN {scope_ids}
                CALL {{
                    MATCH (e:{self.label}) WHERE id(e) = {self.id} RETURN e
                    UNION
                    WITH s RETURN s as e
                }} 
                MATCH r = shortestPath((e)-[*1..16]->(p:{Perm.label}))
                WITH relationships(r) as r, tail(reverse(tail(reverse(nodes(r))))) as n, p
                WHERE type(r[-1]) = "ALLOW"
                    AND (r[-1].scope_id IN {scope_ids} OR NOT EXISTS(r[-1].scope_id))
                    AND all(i IN n WHERE i.__scope_id__ IN {scope_ids} OR NOT EXISTS(i.__scope_id__))
                RETURN p""")
        else:
            cursor = graph.run(
                f'MATCH (e:{self.label}) WHERE id(e) = {self.id} '
                f'MATCH r = shortestPath((e)-[*1..16]->(p:{Perm.label})) '
                f'WITH type(relationships(r)[-1]) = "{ALLOW}" as k, p '
                f'WHERE k RETURN p')
        for record in cursor:
            yield Perm.from_node(record['p'])

    def is_able(self, perm: 'Perm', scope: 'Scope' = None) -> bool:
        if scope:
            scope_ids = cypher_repr([i['i'] for i in graph.run(
                f'MATCH (s:{Scope.label})-[:{SUBSET_OF}*]->(ss:{Scope.label}) '
                f'WHERE id(s) = {scope.id} RETURN id(ss) as i'
            )] + [scope.id, '*'])
            cursor = graph.run(f"""
                MATCH (s:{Scope.label}) WHERE id(s) IN {scope_ids}
                CALL {{
                    MATCH (e:{self.label}) WHERE id(e) = {self.id} RETURN e
                    UNION
                    WITH s RETURN s as e
                }} 
                MATCH (p:{Perm.label}) WHERE id(p) = {perm.id}
                MATCH r = shortestPath((e)-[*1..16]->(p))
                WITH relationships(r) as r, tail(reverse(tail(reverse(nodes(r))))) as n, p
                WHERE type(r[-1]) = "ALLOW"
                    AND (r[-1].scope_id IN {scope_ids} OR NOT EXISTS(r[-1].scope_id))
                    AND all(i IN n WHERE i.__scope_id__ IN {scope_ids} OR NOT EXISTS(i.__scope_id__))
                RETURN id(p) as p""")
        else:
            cursor = graph.run(
                f'MATCH (e:{self.label}) WHERE id(e) = {self.id} '
                f'MATCH (p:{Perm.label}) WHERE id(p) = {perm.id} '
                f'MATCH r = shortestPath((e)-[*1..16]->(p)) '
                f'WITH type(relationships(r)[-1]) = "{ALLOW}" as k, p '
                f'WHERE k RETURN id(p) as p')
        return get_one(cursor) is not None


class Perm(_HasScope, Model):
    pass


class Scope(Model):
    subset_of = ForeignKey('Scope', SUBSET_OF)  # type: Optional['Scope']

    def get_linked_perms(self) -> (Iterable['Perm'], bool):
        cursor = graph.run(
            f'MATCH (s:{self.label}) -[r:{ALLOW}|{DENY}]-> (p:{Perm.label}) '
            f'WHERE id(s) = {self.id} RETURN p, type(r) as r')
        for record in cursor:
            yield Perm.from_node(record['p']), record['r'] == ALLOW

    def link_perm(self, perm: 'Perm', expire_at: datetime = None):
        self.reset_perm(perm)
        graph.run(f'MATCH (s:{self.label}) WHERE id(s) = {self.id} '
                  f'MATCH (p:{Perm.label}) WHERE id(p) = {perm.id} '
                  f'MERGE (s)-[r:{ALLOW}]->(p) '
                  f'{set_expire("r", expire_at)}')

    def reset_perm(self, perm: 'Perm'):
        graph.run(f'MATCH (s:{self.label})-[r:{ALLOW}]->(p:{Perm.label}) '
                  f'WHERE id(s) = {self.id} AND id(p) = {perm.id} DELETE r')

    def reset_all_perms(self):
        graph.run(f'MATCH (s:{self.label})-[r:{ALLOW}]->(p:{Perm.label}) '
                  f'WHERE id(s) = {self.id} DELETE r')


class Ability(_HasScope, Model):
    @classmethod
    def get_available_for_scope(cls, scope: 'Scope') -> Iterable['Ability']:
        cursor = graph.run(
            f'MATCH (a:{cls.label})-[:{EXISTS_IN}|{SUBSET_OF}*]->(s:{Scope.label}) '
            f'WHERE id(s) = {scope.id} RETURN a')
        for record in cursor:
            return Ability.from_node(record['a'])

    def is_perm_supported(self, perm: 'Perm') -> bool:
        cursor = graph.run(f'MATCH (a:{self.label})-[:{SUPPORTS}]->(p:{Perm.label}) '
                           f'WHERE id(a) = {self.id} AND id(p) = {perm.id} RETURN id(p) as i')
        return get_one(cursor) is not None

    def get_supported_perms(self) -> Iterable['Perm']:
        cursor = graph.run(f'MATCH (a:{self.label}) -[:{SUPPORTS}]-> (p:{Perm.label}) '
                           f'WHERE id(a) = {self.id} RETURN p')
        for record in cursor:
            yield Perm.from_node(record['p'])

    def add_perm_support(self, perm: 'Perm', expire_at: datetime = None):
        graph.run(f'MATCH (a:{self.label}) WHERE id(a) = {self.id} '
                  f'MATCH (p:{Perm.label}) WHERE id(p) = {perm.id} '
                  f'MERGE (a)-[r:{SUPPORTS}]->(p) '
                  f'{set_expire("r", expire_at)}')

    def remove_perm_support(self, perm: 'Perm'):
        graph.run(f'MATCH (a:{self.label}) WHERE id(a) = {self.id} '
                  f'MATCH (p:{Perm.label}) WHERE id(p) = {perm.id} '
                  f'MATCH (a)-[r:{SUPPORTS}]->(p) '
                  f'DELETE r')

    def remove_all_supported_perms(self):
        graph.run(f'MATCH (a:{self.label}) WHERE id(a) = {self.id} '
                  f'MATCH (a)-[r:{SUPPORTS}]->(:{Perm.label}) '
                  f'DELETE r')


EnabledAbility = namedtuple('EnabledAbility', 'ability perm_id scope_id')
