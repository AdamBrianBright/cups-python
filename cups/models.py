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
    'AbilityPerm',
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
        cursor = graph.run(
            f'MATCH (i:{self.label}) -[:{EXISTS_IN}]-> (j:{Scope.label}) '
            f'WHERE id(i) = {self.id} RETURN j'
        )
        if record := get_one(cursor):
            return Scope.from_node(record['j'])

    @scope.deleter
    def scope(self) -> None:
        self['__scope_id__'] = None
        self.save(update_fields=['__scope_id__'])
        graph.run(
            f'MATCH (i:{self.label}) -[r:{EXISTS_IN}]-> (:{Scope.label}) '
            f'WHERE id(i) = {self.id} DELETE r'
        )

    @scope.setter
    def scope(self, item: 'NodeType') -> None:
        graph.run(
            f'MATCH (i:{self.label}) -[r:{EXISTS_IN}]-> (:{Scope.label}) '
            f'WHERE id(i) = {self.id} DELETE r'
        )
        self['__scope_id__'] = item.id
        self.save(update_fields=['__scope_id__'])
        graph.run(
            f'MATCH (i:{self.label}) WHERE id(i) = {self.id} '
            f'MATCH (j:{Scope.label}) WHERE id(j) = {item.id} '
            f'MERGE (i) -[:{EXISTS_IN}]-> (j)'
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
        try:
            next(cursor)
        except StopIteration:
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

    def add_to_group(self, group: 'Group'):
        graph.run(f'MATCH (e:{self.label}) WHERE id(e) = {self.id} '
                  f'MATCH (g:{Group.label}) WHERE id(g) = {group.id} '
                  f'MERGE (e)-[:{IS_IN}]->(g)')

    def remove_from_group(self, group: 'Group'):
        graph.run(f'MATCH (e:{self.label})-[r:{IS_IN}]->(g:{Group.label}) '
                  f'WHERE id(e) = {self.id} and id(g) = {group.id} DELETE r')

    def remove_from_all_groups(self):
        graph.run(f'MATCH (e:{self.label})-[r:{IS_IN}]->(g:{Group.label}) '
                  f'WHERE id(e) = {self.id} DELETE r')

    def get_all_activated_abilities(self) -> Iterable['AbilityPerm']:
        cursor = graph.run(f'MATCH (e:{self.label}) WHERE id(e) = {self.id} '
                           f'MATCH (e)-[:{ENABLED}]->(ap:{AbilityPerm.label}) '
                           f'RETURN ap')
        for record in cursor:
            yield AbilityPerm.from_node(record['ap'])

    def get_activated_abilities(self, scope: 'Scope' = None) -> Iterable['Ability']:
        f = {'scope_id': scope.id}
        cursor = graph.run(f'MATCH (e:{self.label}) WHERE id(e) = {self.id} '
                           f'MATCH (e)-[:{ENABLED}]->(ap:{AbilityPerm.label} {encode_dict(f)}) '
                           f'RETURN ap')
        for record in cursor:
            yield AbilityPerm.from_node(record['ap'])

    def activate_ability(self, ability: 'Ability', perm: 'Perm', scope: 'Scope' = None):
        ability.is_scope_supported(scope)

        if perm.id not in [i.id for i in ability.get_supported_perms()]:
            raise ValueError('Permission is not supported by this ability')

        ability_perm = AbilityPerm.get_or_create(
            entity_id=self.id,
            ability_id=ability.id,
            scope_id=scope.id if scope else None,
            default={'perm_id': perm.id},
        )
        ability_perm.ability = ability
        ability_perm.perm = perm
        if scope:
            ability_perm.scope = scope

    def reset_ability(self, ability: 'Ability', scope: 'Scope' = None):
        f = {'ability_id': ability.id, 'scope_id': scope.id if scope else None}
        graph.run(f'MATCH (e:{self.label})-[:{ENABLED}]->(ap:{AbilityPerm.label} {encode_dict(f)}) '
                  f'WHERE id(e) = {self.id} DETACH DELETE ap')

    def reset_ability_in_all_scopes(self, ability: 'Ability'):
        graph.run(f'MATCH (e:{self.label})-[:{ENABLED}]->(ap:{AbilityPerm.label})-[:{RELATED_TO}]->(a:{Ability.label}) '
                  f'WHERE id(e) = {self.id} AND id(a) = {ability.id} '
                  f'DETACH DELETE ap')

    def reset_all_abilities(self):
        graph.run(f'MATCH (e:{self.label})-[:{ENABLED}]->(ap:{AbilityPerm}) '
                  f'WHERE id(e) = {self.id} DETACH DELETE ap')

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

    def link_perm(self, perm: 'Perm', /, scope: 'Scope' = None, allow: bool = True):
        perm.is_scope_supported(scope)
        self.reset_perm(perm, scope=scope)
        f = {'scope_id': scope.id if scope else '*'}
        graph.run(f'MATCH (e:{self.label}) WHERE id(e) = {self.id} '
                  f'MATCH (p:{Perm.label}) WHERE id(p) = {perm.id} '
                  f'MERGE (e)-[:{ALLOW if allow else DENY} {encode_dict(f)}]->(p)')

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
        try:
            next(cursor)
            return True
        except StopIteration:
            return False


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
        self.save(update_fields=['__global__'])
        graph.run(
            f'MATCH (g:{self.label}) WHERE id(g) = {self.id} '
            f'MATCH (e:{Entity.label}) '
            f'MERGE (e)-[:{IS_IN_AUTO}]->(g)')

    def make_optional(self):
        if self.get('__global__') is not True:
            return
        self['__global__'] = True
        self.save(update_fields=['__global__'])
        graph.run(f'MATCH (:{Entity.label})-[r:{IS_IN_AUTO}]->(g:{self.label} '
                  f'WHERE id(g) = {self.id} DELETE r')

    def get_linked_perms(self) -> (Iterable['Perm'], bool):
        cursor = graph.run(
            f'MATCH (e:{self.label}) -[r:{ALLOW}|{DENY}]-> (p:{Perm.label}) '
            f'WHERE id(e) = {self.id} RETURN p, type(r) as r')
        for record in cursor:
            yield Perm.from_node(record['p']), record['r'] == ALLOW

    def link_perm(self, perm: 'Perm', /, allow: bool = True):
        self.reset_perm(perm)
        graph.run(f'MATCH (e:{self.label}) WHERE id(e) = {self.id} '
                  f'MATCH (p:{Perm.label}) WHERE id(p) = {perm.id} '
                  f'MERGE (e)-[:{ALLOW if allow else DENY}]->(p)')

    def link_all_perms(self, /, allow: bool = True):
        self.reset_all_perms()
        graph.run(f'MATCH (e:{self.label}) WHERE id(e) = {self.id} '
                  f'MATCH (p:{Perm.label}) '
                  f'MERGE (e)-[:{ALLOW if allow else DENY}]->(p)')

    def reset_perm(self, perm: 'Perm'):
        graph.run(f'MATCH (e:{self.label})-[r:{ALLOW}|{DENY}]->(p:{Perm.label}) '
                  f'WHERE id(e) = {self.id} AND id(p) = {perm.id} DELETE r')

    def reset_all_perms(self):
        graph.run(f'MATCH (e:{self.label})-[r:{ALLOW}|{DENY}]->(p:{Perm.label}) '
                  f'WHERE id(e) = {self.id} DELETE r')

    def get_allowed_perms(self, scope: 'Scope' = None) -> Iterable['Perm']:
        if scope:
            cursor = graph.run(
                f'MATCH (s:{Scope.label}) WHERE id(s) = {scope.id} '
                f'MATCH (s)-[:{ALLOW}]->(p1:{Perm.label}) '
                f'RETURN p1 as p '
                f'UNION '
                f'MATCH (s:{Scope.label}) WHERE id(s) = {scope.id} '
                f'MATCH (g:{Group.label}) '
                f'WHERE NOT (g)-[:{EXISTS_IN}|{SUBSET_OF}*]->(s) '  # g = Groups in wrong scope
                f'MATCH (e:{self.label}) WHERE id(e) = {self.id} '  # e = current Entity
                f'MATCH (p2:{Perm.label}) WHERE '  # p = Perms...
                f'NOT (p2)<-[:{ALLOW}|{DENY}]-(g) '  # not in g...
                f'AND (NOT (p2)-[:{EXISTS_IN}]->(:{Scope.label}) '
                f' OR (p2)-[:{EXISTS_IN}|{SUBSET_OF}]->(s) '
                f' OR (p2)-[:{EXISTS_IN}|{SUBSET_OF}]->(:{Scope.label})<-[:{SUBSET_OF}*]-(s:{Scope.label}) ) '  # and in right scope
                f'MATCH r = shortestPath((e)-[*1..16]->(p2)) '
                f'WITH type(relationships(r)[-1]) = "{ALLOW}" as k, p2 '
                f'WHERE k '
                f'RETURN p2 as p')
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
            cursor = graph.run(
                f'MATCH (s:{Scope.label}) WHERE id(s) = {scope.id} '
                f'MATCH (s)-[:{ALLOW}]->(p1:{Perm.label}) '
                f'RETURN id(p1) as p '
                f'UNION '
                f'MATCH (s:{Scope.label}) WHERE id(s) = {scope.id} '
                f'MATCH (g:{Group.label}) '
                f'WHERE NOT (g)-[:{EXISTS_IN}|{SUBSET_OF}*]->(s) '  # g = Groups in wrong scope
                f'MATCH (e:{self.label}) WHERE id(e) = {self.id} '  # e = current Entity
                f'MATCH (p2:{Perm.label}) WHERE '  # p = Perms...
                f'NOT (p2)<-[:{ALLOW}|{DENY}]-(g) '  # not in g...
                f'AND (NOT (p2)-[:{EXISTS_IN}]->(:{Scope.label}) '
                f' OR (p2)-[:{EXISTS_IN}|{SUBSET_OF}]->(s) '
                f' OR (p2)-[:{EXISTS_IN}|{SUBSET_OF}]->(:{Scope.label})<-[:{SUBSET_OF}*]-(s:{Scope.label}) ) '  # and in right scope
                f'MATCH r = shortestPath((e)-[*1..16]->(p2)) '
                f'WITH type(relationships(r)[-1]) = "{ALLOW}" as k, p2 '
                f'WHERE k '
                f'RETURN id(p2) as p')
        else:
            cursor = graph.run(
                f'MATCH (e:{self.label}) WHERE id(e) = {self.id} '
                f'MATCH (p:{Perm.label}) WHERE id(p) = {perm.id} '
                f'MATCH r = shortestPath((e)-[*1..16]->(p)) '
                f'WITH type(relationships(r)[-1]) = "{ALLOW}" as k, p '
                f'WHERE k RETURN id(p) as p')
        try:
            next(cursor)
            return True
        except StopIteration:
            return False


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

    def link_perm(self, perm: 'Perm'):
        self.reset_perm(perm)
        graph.run(f'MATCH (s:{self.label}) WHERE id(s) = {self.id} '
                  f'MATCH (p:{Perm.label}) WHERE id(p) = {perm.id} '
                  f'MERGE (s)-[:{ALLOW}]->(p)')

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

    def get_supported_perms(self) -> Iterable['Perm']:
        cursor = graph.run(f'MATCH (a:{self.label}) -[:{SUPPORTS}]-> (p:{Perm.label}) '
                           f'WHERE id(a) = {self.id} RETURN p')
        for record in cursor:
            yield Perm.from_node(record['p'])

    def add_perm_support(self, perm: 'Perm'):
        graph.run(f'MATCH (a:{self.label}) WHERE id(a) = {self.id} '
                  f'MATCH (p:{Perm.label}) WHERE id(p) = {perm.id} '
                  f'MERGE (a)-[:{SUPPORTS}]->(p)')

    def remove_perm_support(self, perm: 'Perm'):
        graph.run(f'MATCH (a:{self.label}) WHERE id(a) = {self.id} '
                  f'MATCH (p:{Perm.label}) WHERE id(p) = {perm.id} '
                  f'MATCH (a)-[r:{SUPPORTS}]->(p) '
                  f'DELETE r')

    def remove_all_supported_perms(self):
        graph.run(f'MATCH (a:{self.label}) WHERE id(a) = {self.id} '
                  f'MATCH (a)-[r:{SUPPORTS}]->(:{Perm.label}) '
                  f'DELETE r')


class AbilityPerm(Model):
    scope = ForeignKey('Scope', WORKS_IN)
    ability = ForeignKey('Ability', RELATED_TO)
    perm = ForeignKey('Perm', ACTIVATED)
