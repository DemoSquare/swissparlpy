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
    
    @property
    def count(self):
        return self.__len__()

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


class SwissParlDataProxy(object):
    def __init__(self, proxy, attribute):
        self.proxy = proxy
        self.attribute = attribute

    def __call__(self):
        return getattr(self.proxy, self.attribute)
