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
