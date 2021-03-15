from asyncio import CancelledError, sleep
from datetime import datetime

import pytz

from cups.db import graph

__all__ = [
    'delete_expired_task',
    'delete_expired_nodes',
    'delete_expired_edges',
]


async def delete_expired_task(interval: int = 60):
    while True:
        try:
            delete_expired_nodes()
            delete_expired_edges()
            await sleep(interval)
        except (KeyboardInterrupt, CancelledError):
            raise
        except Exception:
            pass


def delete_expired_nodes():
    n = datetime.now(tz=pytz.UTC).timestamp()
    graph.run(f'MATCH (i) WHERE i.expire_at < {n} DETACH DELETE i')


def delete_expired_edges():
    n = datetime.now(tz=pytz.UTC).timestamp()
    graph.run(f'MATCH ()-[i]-() WHERE i.expire_at < {n} DELETE i')
