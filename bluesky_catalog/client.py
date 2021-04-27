import json
import warnings

from tiled.catalogs.utils import IndexCallable
from tiled.client.catalog import Catalog

from .common import BlueskyEventStreamMixin, BlueskyRunMixin, CatalogOfBlueskyRunsMixin
from .queries import PartialUID, RawMongo, ScanID


class BlueskyRun(BlueskyRunMixin, Catalog):
    """
    This encapsulates the data and metadata for one Bluesky 'run'.

    This adds for bluesky-specific conveniences to the standard client Catalog.
    """

    def documents(self):
        # (name, doc) pairs are streamed as newline-delimited JSON
        with self._client.stream(
            "GET", f"/documents/{'/'.join(self._path)}"
        ) as response:
            for line in response.iter_lines():
                yield tuple(json.loads(line))

    def __getattr__(self, key):
        """
        Let run.X be a synonym for run['X'] unless run.X already exists.

        This behavior is the same as with pandas.DataFrame.
        """
        # The wisdom of this kind of "magic" is arguable, but we
        # need to support it for backward-compatibility reasons.
        if key in self:
            return self[key]
        raise AttributeError(key)


class BlueskyEventStream(BlueskyEventStreamMixin, Catalog):
    """
    This encapsulates the data and metadata for one 'stream' in a Bluesky 'run'.

    This adds for bluesky-specific conveniences to the standard client Catalog.
    """

    def to_dask(self):
        warnings.warn(
            """Do not use this method.
Instead, set dask or when first creating the Catalog, as in

    >>> catalog = from_uri("...", "dask")

and then read() will return dask objects.""",
            DeprecationWarning,
        )
        return self.new_variation(
            structure_clients=Catalog.DEFAULT_STRUCTURE_CLIENT_DISPATCH["dask"]
        ).read()


class CatalogOfBlueskyRuns(CatalogOfBlueskyRunsMixin, Catalog):
    """
    This adds some bluesky-specific conveniences to the standard client Catalog.

    >>> catalog.scan_id[1234]  # scan_id lookup
    >>> catalog.uid["9acjef"]  # (partial) uid lookup
    >>> catalog[1234]  # automatically do scan_id lookup for positive integer
    >>> catalog["9acjef"]  # automatically do (partial) uid lookup for string
    >>> catalog[-5]  # automatically do catalog.values_indexer[-N] for negative integer
    """

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.scan_id = IndexCallable(self._lookup_by_scan_id)
        self.uid = IndexCallable(self._lookup_by_partial_uid)

    def __getitem__(self, key):
        # For convenience and backward-compatiblity reasons, we support
        # some "magic" here that is helpful in an interactive setting.
        if isinstance(key, str):
            # CASE 1: Interpret key as a uid or partial uid.
            return self._lookup_by_partial_uid(key)
        elif isinstance(key, int):
            if key > 0:
                # CASE 2: Interpret key as a scan_id.
                return self._lookup_by_scan_id(key)
            else:
                # CASE 3: Interpret key as a recently lookup, as in
                # `catalog[-1]` is the latest entry.
                return self.values_indexer[key]

    def _lookup_by_scan_id(self, scan_id):
        results = self.search(ScanID(scan_id, duplicates="latest"))
        if not results:
            raise KeyError(f"No match for scan_id={scan_id}")
        else:
            # By construction there must be only one result. Return it.
            return results.values_indexer[0]

    def _lookup_by_partial_uid(self, partial_uid):
        results = self.search(PartialUID(partial_uid))
        if not results:
            raise KeyError(f"No match for partial_uid {partial_uid}")
        else:
            # By construction there must be only one result. Return it.
            return results.values_indexer[0]

    def search(self, query):
        # For backward-compatiblity, accept a dict and interpret it as a Mongo
        # query against the 'start' documents.
        if isinstance(query, dict):
            query = RawMongo(start=query)
        return super().search(query)
