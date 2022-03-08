import abc
import math

import pyodata
import requests

from swissparlpy import SwissParlError

SERVICE_URL = "https://ws.parlament.ch/odata.svc/"


class SwissParlClient(object):
    def __init__(self, session=None, url=SERVICE_URL):
        if not session:
            session = requests.Session()
        self.url = url
        self.client = pyodata.Client(url, session)
        self.cache = {}
        self.get_overview()

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
            entities.top(rows).count(inline=True), self.get_variables(table)
        )

    def get_data(self, table, filter=None, **kwargs):
        entities = self._filter_entities(self._get_entities(table), filter, **kwargs)
        return SwissParlResponse(entities.count(inline=True), self.get_variables(table))

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

    def get_data_batched(self, table, filter=None, batch_size=50000, **kwargs):
        entities = self._filter_entities(self._get_entities(table), filter, **kwargs)
        count_data = entities.count().execute()
        batch_requests = []
        for i in range(math.ceil(count_data / batch_size)):
            batch_requests.append(
                self._filter_entities(self._get_entities(table), filter, **kwargs)
                .skip(i * batch_size)
                .top(batch_size)
                .count(inline=True)
            )
        return SwissParlBatchedResponse(batch_requests, self.get_variables(table))


class SwissParlResponse(abc.ABC):
    def _setup_proxies(self):
        for e in self.entities:
            row = {k: SwissParlDataProxy(e, k) for k in self.variables}
            self.data.append(row)

    def __len__(self):
        return self.count

    def __iter__(self):
        for row in self.data:
            yield {k: v() for k, v in row.items()}

    def __getitem__(self, key):
        items = self.data[key]
        if isinstance(key, slice):
            return [{k: v() for k, v in i.items()} for i in items]
        return {k: v() for k, v in items.items()}


class SwissParlRequestResponse(SwissParlResponse):
    def __init__(self, entity_request, variables):
        self.entities = entity_request.execute()
        self.count = self.entities.total_count
        self.variables = variables
        self.data = []
        super()._setup_proxies()


class SwissParlBatchedResponse:
    def __init__(self, entity_requests, variables, retries=50) -> None:
        self.entities = []
        self.count = []
        for entity_request in entity_requests:
            entities = self._execute_and_retry(entity_request, retries)
            self.entities.append(entities)
            self.count += entities.total_count
        self.variables = variables
        self.data = []
        super._setup_proxies()

    def _execute_and_retry(self, request, retries):
        trials = 0
        while trials < retries:
            try:
                return request.execute()
            except ConnectionError:
                trials += 1
        raise SwissParlError(f"Could not execute request after {retries} retries")


class SwissParlDataProxy(object):
    def __init__(self, proxy, attribute):
        self.proxy = proxy
        self.attribute = attribute

    def __call__(self):
        return getattr(self.proxy, self.attribute)
