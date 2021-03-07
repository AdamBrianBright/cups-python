import os

from py2neo import Graph

__all__ = [
    'graph',
]

graph = Graph(os.environ.get('CUPS_DB_PROFILE', 'bolt://neo4j:cifrazia@localhost:7687'))
