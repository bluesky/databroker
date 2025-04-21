import keyword
import warnings

from tiled.client.container import DEFAULT_STRUCTURE_CLIENT_DISPATCH, Container
from tiled.client.composite import Composite
from tiled.client.utils import handle_error
from tiled.utils import DictView, OneShotCachedMap, node_repr, Sentinel

from ._common import IPYTHON_METHODS

ONLY_DATA = Sentinel("ONLY_DATA")
ONLY_TIMESTAMPS = Sentinel("ONLY_TIMESTAMPS")

class BlueskyEventStream(Container):
    def __new__(cls, context, *, item, structure_clients, **kwargs):
        # When inheriting from BlueskyEventStream, return the class itself
        if cls is not BlueskyEventStream:
            return super().__new__(cls)

        # Set the version based on the specs
        _cls = BlueskyEventStreamV3 if cls._is_sql(item) else BlueskyEventStreamV2Mongo
        return _cls(context, item=item, structure_clients=structure_clients, **kwargs)

    @staticmethod
    def _is_sql(item):
        for spec in item["attributes"]["specs"]:
            if spec["name"] == "BlueskyEventStream":
                if spec["version"].startswith("3."):
                    return True
                return False


class BlueskyEventStreamV2Mongo(BlueskyEventStream):
    """
    This encapsulates the data and metadata for one 'stream' in a Bluesky 'run'.

    This adds for bluesky-specific conveniences to the standard client Container.
    """

    def __repr__(self):
        stream_name = self.metadata.get("stream_name") or self.item["id"]
        return f"<{type(self).__name__} {set(self)!r} stream_name={stream_name!r}>"

    @property
    def descriptors(self):
        return self.metadata["descriptors"]

    @property
    def _descriptors(self):
        # For backward-compatibility.
        # We do not normally worry about backward-compatibility of _ methods, but
        # for a time databroker.v2 *only* have _descriptors and not descriptors,
        # and I know there is useer code that relies on that.
        warnings.warn("Use `.descriptors` instead of `._descriptors`.", stacklevel=2)
        return self.descriptors

    def __getattr__(self, key):
        """
        Let run.X be a synonym for run['X'] unless run.X already exists.

        This behavior is the same as with pandas.DataFrame.
        """
        # The wisdom of this kind of "magic" is arguable, but we
        # need to support it for backward-compatibility reasons.
        if key in IPYTHON_METHODS:
            raise AttributeError(key)
        if key in self:
            return self[key]
        raise AttributeError(key)

    def __dir__(self):
        # Build a list of entries that are valid attribute names
        # and add them to __dir__ so that they tab-complete.
        tab_completable_entries = [
            entry for entry in self if (entry.isidentifier() and (not keyword.iskeyword(entry)))
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
        return self.new_variation(structure_clients=DEFAULT_STRUCTURE_CLIENT_DISPATCH["dask"]).read()


class BlueskyEventStreamV2SQL(OneShotCachedMap):
    def __init__(self, internal_dict, metadata=None):
        super().__init__(internal_dict)
        self.metadata = metadata or {}

    def __repr__(self):
        stream_name = self.metadata.get("stream_name")
        return f"<BlueskyEventStream {set(self)!r} stream_name={stream_name!r}>"

    def __getitem__(self, key):
        if "/" in key:
            key, rest = key.split("/", 1)
            return self[key][rest]

        return super().__getitem__(key)

    @staticmethod
    def format_config(config_client, timestamp=False):
        records = config_client.read().to_list()
        values = defaultdict(dict)
        for rec in records:
            if (rec.get("object_name") is not None) and (rec.get("value") is not None):
                values[rec["object_name"]][rec["data_key"]] = (
                    VirtualArrayClient(rec["timestamp"]) if timestamp else VirtualArrayClient(rec["value"])
                )
        result = {k: ConfigDatasetClient(v) for k, v in values.items()}
        return VirtualContainer(result)

    @classmethod
    def from_container_and_config(cls, stream_client, config_client):
        stream_parts = set(stream_client.parts)
        data_keys = [k for k in stream_parts if k != "internal"]
        ts_keys = ["time"]
        if "internal" in stream_parts:
            internal_cols = stream_client.parts["internal"].columns
            data_keys += [col for col in internal_cols if col != "seq_num" and not col.startswith("ts_")]
            ts_keys += [col for col in internal_cols if col.startswith("ts_")]
        internal_dict = {
            # "data": lambda: stream_client.to_dataset(*sorted(set(data_keys))),
            # "timestamps": lambda: stream_client.to_dataset(*ts_keys),
            "data": lambda: CompositeDatasetClient(stream_client, data_keys),
            "timestamps": lambda: CompositeDatasetClient(stream_client, ts_keys),
            "config": lambda: cls.format_config(config_client),
            "config_timestamps": lambda: cls.format_config(config_client, timestamp=True),
        }

        # Construct the metadata
        metadata = {"descriptors": [], "stream_name": stream_client.item["id"], **stream_client.metadata}

        return cls(internal_dict, metadata=metadata)


class ConfigDatasetClient(DictView):
    def __repr__(self):
        tiled_repr = node_repr(self, self._internal_dict.keys())
        return tiled_repr.replace(type(self).__name__, "DatasetClient")

    def read(self):
        d = {k: {"dims": "time", "data": v.read()} for k, v in self._internal_dict.items()}
        return xarray.Dataset.from_dict(d)


class CompositeDatasetClient(DictView):
    def __init__(self, node, keys):
        super().__init__({k: lambda _k=k: node[_k] for k in sorted(set(keys))})
        self._node = node

    def __repr__(self):
        tiled_repr = node_repr(self, self._internal_dict.keys())
        return tiled_repr.replace(type(self).__name__, "DatasetClient")

    def read(self):
        return self._node.read(variables=list(self.keys()))


class VirtualContainer(DictView):
    def __repr__(self):
        tiled_repr = node_repr(self, self._internal_dict.keys())
        return tiled_repr.replace(type(self).__name__, "ContainerClient")

    def __getitem__(self, key):
        if "/" in key:
            key, rest = key.split("/", 1)
            return self[key][rest]

        return super().__getitem__(key)


class VirtualArrayClient:
    def __init__(self, data, dims=None):
        # Ensure data is an array-like object
        if not hasattr(data, "__iter__") or isinstance(data, str):
            data = [data]
        if not hasattr(data, "__array__"):
            data = numpy.asanyarray(data)

        self._data = data
        self._dims = dims

    def __getitem__(self, slice):
        return self.read(slice)

    def __repr__(self):
        attrs = {"shape": self.shape, "dtype": self.dtype}
        if dims := self.dims:
            attrs["dims"] = dims
        return "<ArrayClient" + "".join(f" {k}={v}" for k, v in attrs.items()) + ">"

    def read(self, slice=None):
        return self._data if slice is None else self._data[slice]

    @property
    def size(self):
        return self._data.size

    @property
    def shape(self):
        return self._data.shape

    @property
    def dtype(self):
        return self._data.dtype

    @property
    def dims(self):
        return self._dims


class BlueskyEventStreamV3(BlueskyEventStream, Composite):

    def __repr__(self):
        stream_name = self.metadata.get("stream_name") or self.item["id"]
        keys = (k for k in self if not k.startswith("ts_"))
        return f"<BlueskyEventStream {set(keys)!r} stream_name={stream_name!r}>"
