import math
import logging

import pyodata
import requests
import tqdm
import os
import json

from swissparlpy import SwissParlError

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
        return SwissParlRequestResponse(
            entities.top(rows), self.get_variables(table)
        )

    def get_data(self, table, filter=None, **kwargs):
        entities = self._filter_entities(self._get_entities(table), filter, **kwargs)
        return SwissParlRequestResponse(
            entities, self.get_variables(table)
        )

    def get_count(self, table, filter=None, **kwargs):
        entities = self._filter_entities(self._get_entities(table), filter, **kwargs)
        return entities.count().execute()


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

    def get_data_batched(
        self, table, filter=None, batch_size=50000, use_disk=False, **kwargs
    ):
        entities = self._filter_entities(self._get_entities(table), filter, **kwargs)
        count_data = entities.count().execute()
        batch_requests = []
        for i in range(math.ceil(count_data / batch_size)):
            batch_requests.append(
                self._filter_entities(self._get_entities(table), filter, **kwargs)
                .skip(i * batch_size)
                .top(batch_size)
            )
        logger.debug(
            """Launching batch request for data of table %s with batch size %i
                num requests: %i for data of size %i
            """, 
            table, batch_size, len(batch_requests), count_data
        )
        return SwissParlBatchedResponse(
            batch_requests, self.get_variables(table), use_disk=use_disk
        )


class SwissParlResponse:
    def __init__(self, entities, variables):
        self.entities = entities
        self.variables = variables
        self._setup_proxies()

    def _setup_proxies(self):
        self.data = []
        for e in self.entities:
            row = {k: SwissParlDataProxy(e, k) for k in self.variables}
            self.data.append(row)

    def __len__(self):
        return len(self.entities)

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
        entities = entity_request.execute()
        super().__init__(entities, variables)


class SwissParlBatchedResponse(SwissParlResponse):
    def __init__(
        self, entity_requests, variables, retries=10, use_disk=False, save_loc="."
    ) -> None:
        entities = []
        self.savefiles = []
        for i, entity_request in tqdm.tqdm(enumerate(entity_requests)):
            batch_entities = self._execute_and_retry(entity_request, retries)
            logger.debug("Batch %i successful", i)
            if use_disk:
                file_path = os.path.join(save_loc, f"batch{i}")
                with open(file_path, "w") as file:
                    json.dump(
                        [
                            {k: str(getattr(entity, k)) for k in variables}
                            for entity in batch_entities
                        ],
                        file,
                    )
                self.savefiles.append(file_path)
            else:
                entities.extend(batch_entities)

        super().__init__(entities, variables)

    def _execute_and_retry(self, request, retries):
        trials = 0
        while trials < retries:
            try:
                return request.execute()
            except ConnectionError:
                logger.debug("Retrying request... num retries: %i", trials)
                trials += 1
            except pyodata.exceptions.HttpError:
                logger.info("HTTP error, retrying...num retries: %i", trials)
                trials += 1

        raise SwissParlError("Could not execute request after %i retries", retries)


class SwissParlDataProxy(object):
    def __init__(self, proxy, attribute):
        self.proxy = proxy
        self.attribute = attribute

    def __call__(self):
        return getattr(self.proxy, self.attribute)
