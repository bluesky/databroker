import functools
import keyword
import warnings
from collections import defaultdict
from typing import Optional

import numpy
import xarray
from tiled.client.composite import CompositeClient
from tiled.client.container import DEFAULT_STRUCTURE_CLIENT_DISPATCH, Container
from tiled.utils import DictView, OneShotCachedMap, Sentinel, node_repr

from ._common import IPYTHON_METHODS

DATAVALUES = Sentinel("DATAVALUES")
TIMESTAMPS = Sentinel("TIMESTAMPS")


class BlueskyEventStream(Container):
    _ipython_display_ = None
    _repr_mimebundle__ = None

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
        return f"<BlueskyEventStream {set(self)!r} stream_name={stream_name!r}>"

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

    @classmethod
    def from_stream_client(cls, stream_client, metadata=None):
        stream_parts = set(stream_client.base.keys())
        data_keys = [k for k in stream_parts if k != "internal"]
        ts_keys = ["time"]
        if "internal" in stream_parts:
            internal_cols = stream_client.base["internal"].columns
            data_keys += [col for col in internal_cols if col != "seq_num" and not col.startswith("ts_")]
            ts_keys += [col for col in internal_cols if col.startswith("ts_")]

        # Construct clients for the configuration data
        cf_vals, cf_time = defaultdict(dict), defaultdict(dict)
        if config := stream_client.metadata.get("configuration", {}):
            updates = stream_client.metadata.get("_config_updates", [])
            for obj_name, obj in config.items():
                for key in obj["data"].keys():
                    _vs, _ts = [obj["data"][key]], [obj["timestamps"][key]]

                    # Add values and timestamps from config_updates
                    for upd in updates:
                        if upd_config := upd.get("configuration", {}):
                            _vs.append(upd_config.get("data", {}).get(key))
                            _ts.append(upd_config.get("timestamps", {}).get(key))

                    cf_vals[obj_name][key] = VirtualArrayClient(_vs)
                    cf_time[obj_name][key] = VirtualArrayClient(_ts)

        internal_dict = {
            "data": lambda: CompositeSubsetClient(stream_client, data_keys),
            "timestamps": lambda: CompositeSubsetClient(stream_client, ts_keys),
            "config": lambda: VirtualContainer({k: ConfigDatasetClient(v) for k, v in cf_vals.items()}),
            "config_timestamps": lambda: VirtualContainer({k: ConfigDatasetClient(v) for k, v in cf_time.items()}),
        }

        # Construct the metadata
        metadata = {
            "descriptors": [],
            "stream_name": stream_client.item["id"],
            **stream_client.metadata,
            **(metadata or {}),
        }

        return cls(internal_dict, metadata=metadata)

    @functools.cached_property
    def descriptors(self):
        # Go back to the BlueskyRun node and requests the documents
        bs_run_node = self["data"].parent.parent  # the path is: bs_run_node/streams/current_stream
        stream_name = self.metadata.get("stream_name") or self["data"].item["id"]
        return [
            doc for name, doc in bs_run_node.documents() if name == "descriptor" and doc["name"] == stream_name
        ]

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

    def read(self, *args, **kwargs):
        """Read the data from the stream.

        This is a shortcut for reading the 'data' (as opposed to timestamps or config).
        """
        return self["data"].read(*args, **kwargs)


class ConfigDatasetClient(DictView):
    def __repr__(self):
        tiled_repr = node_repr(self, self._internal_dict.keys())
        return tiled_repr.replace(type(self).__name__, "DatasetClient")

    def read(self):
        # Delay this import for fast startup. In some cases only metadata
        # is handled, and we can avoid the xarray import altogether.

        d = {k: {"dims": "time", "data": v.read()} for k, v in self._internal_dict.items()}
        return xarray.Dataset.from_dict(d)


class CompositeSubsetClient(CompositeClient):
    """A composite client with only a subset of its keys exposed."""

    def __init__(self, client, keys=None):
        super().__init__(context=client.context, item=client.item, structure_clients=client.structure_clients)
        self._keys = keys or list(client.keys())

    def __repr__(self):
        return node_repr(self, self._keys).replace(type(self).__name__, "DatasetClient")

    def _keys_slice(self, start, stop, direction, page_size: Optional[int] = None, **kwargs):
        yield from self._keys[start : stop : -1 if direction < 0 else 1]  # noqa: #203

    def _items_slice(self, start, stop, direction, page_size: Optional[int] = None, **kwargs):
        for key in self._keys[start : stop : -1 if direction < 0 else 1]:  # noqa: #203
            yield key, self[key]

    def __iter__(self):
        yield from self._keys

    def __getitem__(self, key):
        if key in self._keys:
            return super().__getitem__(key)
        raise KeyError(key)

    def __len__(self):
        return len(self._keys)

    def __contains__(self, key):
        return key in self._keys

    def read(self, variables=None, dim0=None):
        variables = set(self._keys).intersection(variables or self._keys)

        return super().read(variables, dim0=dim0)


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
        # Delay this import for fast startup. In some cases only metadata
        # is handled, and we can avoid the numpy import altogether.

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


class BlueskyEventStreamV3(BlueskyEventStream, CompositeClient):
    def __repr__(self):
        stream_name = self.metadata.get("stream_name") or self.item["id"]
        return f"<BlueskyEventStream {self._var_keys!r} stream_name={stream_name!r}>"

    @property
    def _var_keys(self):
        return {k for k in self if not k.startswith("ts_") and k != "seq_num"}

    @property
    def _ts_keys(self):
        return {k for k in self if k.startswith("ts_")}

    def read(self, variables=(DATAVALUES,), dim0=None):
        if DATAVALUES in variables:
            variables = self._var_keys.union(variables) - {DATAVALUES}
        if TIMESTAMPS in variables:
            variables = self._ts_keys.union(variables) - {TIMESTAMPS}

        return super().read(variables=variables, dim0=dim0)

    @functools.cached_property
    def descriptors(self):
        # Go back to the BlueskyRun node and requests the documents
        stream_name = self.metadata.get("stream_name") or self.item["id"]
        bs_run_node = self.parent.parent  # the path is: bs_run_node/streams/current_stream
        return [
            doc for name, doc in bs_run_node.documents() if name == "descriptor" and doc["name"] == stream_name
        ]
