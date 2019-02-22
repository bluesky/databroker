from intake import Catalog


class Broker:
    def __init__(self, uri, source):
        catalog = Catalog(uri)
        if source is not None:
            catalog = catalog[source]
        self._catalog = catalog

    def __call__(self, text_search=None, **kwargs):
        data_key = kwargs.pop('data_key', None)
        return Results(self, self._catalog.search(kwargs), data_key)

    def __getitem__(self, key):
        entry = self._catalog[key]
        return Header(entry, self)


class Header:
    def __init__(self, entry, broker):
        self.start = entry.metadata['start']
        self.stop = entry.metadata['stop']
        self._descriptors = None  # Fetch lazily in property.
        self.ext = None  # TODO
        self.db = broker

    @property
    def descriptors(self):
        if self._descriptors is None:
            # TODO Fetch descriptors
            ...
        return self._descriptors

    # These methods mock part of the dict interface. It has been proposed that
    # we might remove them for 1.0.

    def __getitem__(self, k):
        if k in ('start', 'descriptors', 'stop', 'ext'):
            return getattr(self, k)
        else:
            raise KeyError(k)

    def get(self, *args, **kwargs):
        return getattr(self, *args, **kwargs)

    def items(self):
        for k in self.keys():
            yield k, getattr(self, k)

    def values(self):
        for k in self.keys():
            yield getattr(self, k)

    def keys(self):
        for k in ('start', 'descriptors', 'stop', 'ext'):
            yield k

    def __iter__(self):
        return self.keys()

class Results:
    """
    Iterable object encapsulating a results set of Headers

    Parameters
    ----------
    catalog : Catalog
        search results
    data_key : string or None
        Special query parameter that filters results
    """
    def __init__(self, broker, catalog, data_key):
        self._broker = broker
        self._catalog = catalog
        self._data_key = data_key

    def __iter__(self):
        # TODO walk() fails. We should probably support Catalog.items().
        for uid, entry in self._catalog._entries.items():
            header = Header(entry, self._broker)
            if self._data_key is None:
                yield header
            else:
                # Only include this header in the result if `data_key` is found
                # in one of its descriptors' data_keys.
                for descriptor in header.descriptors:
                    if self._data_key in descriptor['data_keys']:
                        yield header
                        break
