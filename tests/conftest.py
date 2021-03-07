import asyncio
import logging
import platform

from py2neo import Graph
from pytest import fixture

# noinspection PyArgumentList
# logging.basicConfig(level='DEBUG', force=True)


@fixture(scope='session')
def event_loop():
    if platform.system() == 'Windows':
        # noinspection PyUnresolvedReferences
        asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())
    return asyncio.get_event_loop()


@fixture(scope='session')
def graph() -> Graph:
    import cups.db
    return cups.db.graph


@fixture(scope='function')
def clear_db(graph):
    graph.delete_all()
    yield
    graph.delete_all()
