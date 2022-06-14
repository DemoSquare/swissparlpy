import itertools
import logging
import math

import pyodata
import tqdm

from swissparlpy import SERVICE_URL
from swissparlpy.client import SwissParlClient
from swissparlpy.errors import SwissParlError
from swissparlpy.response import SwissParlResponse

logger = logging.getLogger(__name__)


class BatchSwissParlClient(SwissParlClient):
    def __init__(
        self, session=None, url=SERVICE_URL, batch_size=1000, retries=10, verbose=True
    ):
        super().__init__(session, url)
        if batch_size < 1000:
            logger.warning("A batch size of less than 1000 will result in lost data!")

        self.batch_size = batch_size
        self.retries = max(0, retries)
        self.verbose = verbose

    def _get_batch_queries(self, table, filter, data_count, **kwargs):
        queries = []

        for i in range(math.ceil(data_count / self.batch_size)):
            queries.append(
                self._filter_entities(self._get_entities(table), filter, **kwargs)
                .skip(i * self.batch_size)
                .top(self.batch_size)
            )
        logger.debug(
            """Launching batch request for data of table %s with batch size %i
                num requests: %i for data of size %i
            """,
            table,
            self.batch_size,
            len(queries),
            data_count,
        )
        return queries

    def _execute_and_retry(self, query):
        trials = 0
        while trials < self.retries:
            try:
                return query.execute()
            except ConnectionError | pyodata.exceptions.HttpError:
                logger.debug("Retrying request... num retries: %i", trials)
                trials += 1

        raise SwissParlError("Could not execute request after %i retries", self.retries)

    def get_data(self, table, filter=None, **kwargs):
        data_count = (
            self._filter_entities(self._get_entities(table), filter, **kwargs)
            .count()
            .execute()
        )

        # No need for batching
        if data_count <= self.batch_size:
            return SwissParlResponse(
                self._filter_entities(
                    self._get_entities(table), filter, **kwargs
                ).execute(),
                self.get_variables(table),
            )

        queries = self._get_batch_queries(table, filter, data_count, **kwargs)
        entities = itertools.chain.from_iterable(
            self._execute_and_retry(query)
            for query in tqdm.tqdm(queries, disable=not self.verbose)
        )

        return SwissParlResponse(list(entities), self.get_variables(table))

