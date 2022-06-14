"""Client for Swiss parliament API"""

__version__ = "0.2.1"
__all__ = ["client", "errors"]

SERVICE_URL = "https://ws.parlament.ch/odata.svc/"

from .errors import SwissParlError  # noqa
from .client import SwissParlClient, BatchSwissParlClient
from pyodata.v2.service import GetEntitySetFilter as filter  # noqa


client = SwissParlClient(url=SERVICE_URL)


def get_tables():
    return client.get_tables()


def get_variables(table):
    return client.get_variables(table)


def get_overview():
    return client.get_overview()


def get_glimpse(table, rows=5):
    return client.get_glimpse(table, rows)


def get_count(table, filter=None, **kwargs):  # noqa
    return client.get_count(table, filter, **kwargs)


def get_data(table, filter=None, batch_size=1000, retries=10, verbose=True, **kwargs):  # noqa
    return BatchSwissParlClient(
        url=SERVICE_URL, batch_size=batch_size, retries=retries, verbose=verbose
    ).get_data(table, filter, **kwargs)
