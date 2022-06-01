import collections.abc
import keyword
import warnings

import msgpack
from tiled.adapters.utils import IndexCallable
from tiled.client.node import Node
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
# There are methods that IPython will try to call.
# We special-case them because we want to avoid the getattr
# resulting in an unnecessary network hit just to raise
# AttributeError.
_IPYTHON_METHODS = {"_ipython_canary_method_should_not_exist_", "_repr_mimebundle_"}


class BlueskyRun(BlueskyRunMixin, Node):
    """
    This encapsulates the data and metadata for one Bluesky 'run'.

    This adds for bluesky-specific conveniences to the standard client Node.
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
        # Access internal object context._client here because
        # Context does not yet expose a streaming API.
        request = self.context._client.build_request(
            "GET",
            f"/documents/{'/'.join(list(self.context.path_parts) + list(self._path))}",
            params={"fill": fill},
            headers={"Accept": "application/x-msgpack"},
        )
        response = self.context._client.send(request, stream=True)
        try:
            if response.is_error:
                response.read()
                handle_error(response)
            unpacker = msgpack.Unpacker()
            for chunk in response.iter_bytes():
                unpacker.feed(chunk)
                for name, doc in unpacker:
                    yield (name, _document_types[name](doc))
        finally:
            response.close()

    def __getattr__(self, key):
        """
        Let run.X be a synonym for run['X'] unless run.X already exists.

        This behavior is the same as with pandas.DataFrame.
        """
        # The wisdom of this kind of "magic" is arguable, but we
        # need to support it for backward-compatibility reasons.
        if key in _IPYTHON_METHODS:
            raise AttributeError(key)
        if key in self:
            return self[key]
        raise AttributeError(key)

    def __dir__(self):
        # Build a list of entries that are valid attribute names
        # and add them to __dir__ so that they tab-complete.
        tab_completable_entries = [
            entry
            for entry in self
            if (entry.isidentifier() and (not keyword.iskeyword(entry)))
        ]
        return super().__dir__() + tab_completable_entries

    def describe(self):
        "For back-compat with intake-based BlueskyRun"
        warnings.warn(
            "This will be removed. Use .metadata directly instead of describe()['metadata'].",
            DeprecationWarning,
        )
        return {"metadata": self.metadata}

    def __call__(self):
        warnings.warn(
            "Do not call a BlueskyRun. For now this returns self, for "
            "backward-compatibility. but it will be removed in a future "
            "release.",
            DeprecationWarning,
            stacklevel=2,
        )
        return self

    def read(self):
        raise NotImplementedError(
            "Reading any entire run is not supported. "
            "Access a stream in this run and read that."
        )

    to_dask = read


class BlueskyEventStream(BlueskyEventStreamMixin, Node):
    """
    This encapsulates the data and metadata for one 'stream' in a Bluesky 'run'.

    This adds for bluesky-specific conveniences to the standard client Node.
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

    def __getattr__(self, key):
        """
        Let run.X be a synonym for run['X'] unless run.X already exists.

        This behavior is the same as with pandas.DataFrame.
        """
        # The wisdom of this kind of "magic" is arguable, but we
        # need to support it for backward-compatibility reasons.
        if key in _IPYTHON_METHODS:
            raise AttributeError(key)
        if key in self:
            return self[key]
        raise AttributeError(key)

    def __dir__(self):
        # Build a list of entries that are valid attribute names
        # and add them to __dir__ so that they tab-complete.
        tab_completable_entries = [
            entry
            for entry in self
            if (entry.isidentifier() and (not keyword.iskeyword(entry)))
        ]
        return super().__dir__() + tab_completable_entries

    def read(self, *args, **kwargs):
        """
        Shortcut for reading the 'data' (as opposed to timestamps or config).

        That is:

        >>> stream.read(...)

        is equivalent to

        >>> stream["data"].read(...)
        """
        return self["data"].read(*args, **kwargs)

    def to_dask(self):
        warnings.warn(
            """Do not use this method.
Instead, set dask or when first creating the client, as in

    >>> catalog = from_uri("...", "dask")

and then read() will return dask objects.""",
            DeprecationWarning,
            stacklevel=2,
        )
        return self.new_variation(
            structure_clients=Node.DEFAULT_STRUCTURE_CLIENT_DISPATCH["dask"]
        ).read()


class CatalogOfBlueskyRuns(CatalogOfBlueskyRunsMixin, Node):
    """
    This adds some bluesky-specific conveniences to the standard client Node.

    >>> catalog.scan_id[1234]  # scan_id lookup
    >>> catalog.uid["9acjef"]  # (partial) uid lookup
    >>> catalog[1234]  # automatically do scan_id lookup for positive integer
    >>> catalog["9acjef"]  # automatically do (partial) uid lookup for string
    >>> catalog[-5]  # automatically do catalog.values()[-N] for negative integer
    """

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.scan_id = IndexCallable(self._lookup_by_scan_id)
        self.uid = IndexCallable(self._lookup_by_partial_uid)
        self._v1 = None

    def __getitem__(self, key):
        # For convenience and backward-compatiblity reasons, we support
        # some "magic" here that is helpful in an interactive setting.
        if isinstance(key, str):
            # CASE 1: Interpret key as a uid or partial uid.
            if len(key) == 36:
                # This looks like a full uid. Try direct lookup first.
                try:
                    return super().__getitem__(key)
                except KeyError:
                    # Fall back to partial uid lookup below.
                    pass
            return self._lookup_by_partial_uid(key)
        elif isinstance(key, int):
            if key > 0:
                # CASE 2: Interpret key as a scan_id.
                return self._lookup_by_scan_id(key)
            else:
                # CASE 3: Interpret key as a recently lookup, as in
                # `catalog[-1]` is the latest entry.
                return self.values()[key]
        elif isinstance(key, slice):
            if (key.start is None) or (key.start >= 0):
                raise ValueError(
                    "For backward-compatibility reasons, slicing here "
                    "is limited to negative indexes. "
                    "Use .values() to slice how you please."
                )
            return self.values()[key]
        elif isinstance(key, collections.abc.Iterable):
            # We know that isn't a str because we check that above.
            # Recurse.
            return [self[item] for item in key]
        else:
            raise ValueError(
                "Indexing expects a string, an integer, or a collection of strings and/or integers."
            )

    def _lookup_by_scan_id(self, scan_id):
        results = self.search(ScanID(scan_id, duplicates="latest"))
        if not results:
            raise KeyError(f"No match for scan_id={scan_id}")
        else:
            # By construction there must be only one result. Return it.
            return results.values().first()

    def _lookup_by_partial_uid(self, partial_uid):
        results = self.search(PartialUID(partial_uid))
        if not results:
            raise KeyError(f"No match for partial_uid {partial_uid}")
        else:
            # By construction there must be only one result. Return it.
            return results.values().first()

    def get_serializer(self):
        from tiled.server.app import get_root_tree

        if self.context.app is None:
            raise NotImplementedError("Only works on local application.")
        tree = self.context.app.dependency_overrides[get_root_tree]()
        return tree.get_serializer()

    def search(self, query):
        # For backward-compatiblity, accept a dict and interpret it as a Mongo
        # query against the 'start' documents.
        if isinstance(query, dict):
            query = RawMongo(start=query)
        return super().search(query)

    @property
    def v1(self):
        "Accessor to legacy interface."
        if self._v1 is None:
            from .v1 import Broker

            self._v1 = Broker(self)
        return self._v1
