"""Client for Swiss parliament API"""

__version__ = "0.2.1"
__all__ = ["client", "errors"]

from .errors import SwissParlError  # noqa
from .client import SwissParlClient
from pyodata.v2.service import GetEntitySetFilter as filter  # noqa


def get_tables():
    client = SwissParlClient()
    return client.get_tables()


def get_variables(table):
    client = SwissParlClient()
    return client.get_variables(table)


def get_overview():
    client = SwissParlClient()
    return client.get_overview()


def get_glimpse(table, rows=5):
    client = SwissParlClient()
    return client.get_glimpse(table, rows)


def get_data(table, filter=None, **kwargs):  # noqa
    client = SwissParlClient()
    return client.get_data(table, filter, **kwargs)


def get_data_batched(table, filter=None, batch_size=50000, **kwargs):  # noqa
    client = SwissParlClient()
    return client.get_data_batched(table, filter, batch_size, **kwargs)


def get_count(table, filter=None, **kwargs):  # noqa
    client = SwissParlClient()
    return client.get_count(table, filter, **kwargs)
