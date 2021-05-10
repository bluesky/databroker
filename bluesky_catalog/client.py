import warnings

import httpx
import msgpack
from tiled.catalogs.utils import IndexCallable
from tiled.client.catalog import Catalog
from tiled.client.utils import handle_error

from .common import BlueskyEventStreamMixin, BlueskyRunMixin, CatalogOfBlueskyRunsMixin
from .queries import PartialUID, RawMongo, ScanID
from .document import Start, Stop, Descriptor, EventPage, DatumPage, Resource


_document_types = {
    "start": Start,
    "stop": Stop,
    "descriptor": Descriptor,
    "event_page": EventPage,
    "datum_page": DatumPage,
    "resource": Resource,
}


class BlueskyRun(BlueskyRunMixin, Catalog):
    """
    This encapsulates the data and metadata for one Bluesky 'run'.

    This adds for bluesky-specific conveniences to the standard client Catalog.
    """

    @property
    def start(self):
        """
        The Run Start document. A convenience alias:

        >>> run.start is run.metadata["start"]
        True
        """
        return self.metadata["start"]

    @property
    def stop(self):
        """
        The Run Stop document. A convenience alias:

        >>> run.stop is run.metadata["stop"]
        True
        """
        return self.metadata["stop"]

    def documents(self, fill=False):
        # For back-compat with v2:
        if fill == "yes":
            fill = True
        elif fill == "no":
            fill = False
        elif fill == "delayed":
            raise NotImplementedError("fill='delayed' is not supported")
        else:
            fill = bool(fill)
        # (name, doc) pairs are streamed as newline-delimited JSON
        if isinstance(self._client, httpx.AsyncClient):
            yield from self._stream_async(fill)
        else:
            yield from self._stream_sync(fill)

    def _stream_async(self, fill):
        # HACK! Revisit this once we refactor the generic "service" part
        # out from HTTP.

        import asyncio

        async def drain():
            documents = []
            async with self._client.stream(
                "GET",
                f"/documents/{'/'.join(self._path)}",
                params={"fill": fill},
                headers={"Accept": "application/x-msgpack"},
            ) as response:
                if response.is_error:
                    response.read()
                    handle_error(response)
                unpacker = msgpack.Unpacker()
                async for chunk in response.aiter_raw():
                    unpacker.feed(chunk)
                    for name, doc in unpacker:
                        # This will decode as [name, doc]. We want (name, doc).
                        documents.append((name, _document_types[name](doc)))
            return documents

        for item in asyncio.run(drain()):
            yield item

    def _stream_sync(self, fill):
        with self._client.stream(
            "GET",
            f"/documents/{'/'.join(self._path)}",
            params={"fill": fill},
            headers={"Accept": "application/x-msgpack"},
        ) as response:
            if response.is_error:
                response.read()
                handle_error(response)
            unpacker = msgpack.Unpacker()
            for chunk in response.iter_raw():
                unpacker.feed(chunk)
                for name, doc in unpacker:
                    yield (name, _document_types[name](doc))

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

    @property
    def descriptors(self):
        return self.metadata["descriptors"]

    @property
    def _descriptors(self):
        # For backward-compatibility.
        # We do not normally worry about backward-compatibility of _ methods, but
        # for a time databroker.v2 *only* have _descriptors and not descriptros,
        # and I know there is useer code that relies on that.
        warnings.warn("Use .descriptors instead of ._descriptors.", stacklevel=2)
        return self.descriptors

    def read(self):
        """
        Shortcut for reading the 'data' (as opposed to timestamps or config).

        That is:

        >>> stream.read()

        is equivalent to

        >>> stream["data"].read()
        """
        return self["data"].read()

    def to_dask(self):
        warnings.warn(
            """Do not use this method.
Instead, set dask or when first creating the Catalog, as in

    >>> catalog = from_uri("...", "dask")

and then read() will return dask objects.""",
            DeprecationWarning,
            stacklevel=2,
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
