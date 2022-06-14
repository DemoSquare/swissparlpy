import json
import logging
import math
import os

import pyodata
import requests
import tqdm

from swissparlpy import SwissParlError
from swissparlpy.response import SwissParlResponse

SERVICE_URL = "https://ws.parlament.ch/odata.svc/"
logger = logging.getLogger(__name__)


class SwissParlClient(object):
    def __init__(self, session=None, url=SERVICE_URL):
        if not session:
            session = requests.Session()
        self.url = url
        self.client = pyodata.Client(url, session)
        self.cache = {}
        self.get_overview()

    def _get_entities(self, table):
        return getattr(self.client.entity_sets, table).get_entities()

    def _filter_entities(self, entities, filter=None, **kwargs):
        if filter and callable(filter):
            entities = entities.filter(filter(entities))
        elif filter:
            entities = entities.filter(filter)
        if kwargs:
            entities = entities.filter(**kwargs)
        return entities

    def get_tables(self):
        if self.cache:
            return list(self.cache.keys())
        return [es.name for es in self.client.schema.entity_sets]

    def get_variables(self, table):
        if self.cache and table in self.cache:
            return self.cache[table]
        return [p.name for p in self.client.schema.entity_type(table).proprties()]

    def get_overview(self):
        if self.cache:
            return self.cache
        self.cache = {}
        for t in self.get_tables():
            self.cache[t] = self.get_variables(t)
        return self.cache

    def get_glimpse(self, table, rows=5):
        entities = self._get_entities(table)
        return SwissParlResponse(
            entities.top(rows).execute(), self.get_variables(table)
        )

    def get_data(self, table, filter=None, **kwargs):
        entities = self._filter_entities(self._get_entities(table), filter, **kwargs)
        return SwissParlResponse(
            entities.execute(), self.get_variables(table)
        )

    def get_count(self, table, filter=None, **kwargs):
        entities = self._filter_entities(self._get_entities(table), filter, **kwargs)
        return entities.count().execute()

