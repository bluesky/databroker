import json
import keyword
import warnings
from collections import defaultdict
from datetime import datetime

import numpy
import xarray
from tiled.client.container import Container
from tiled.client.utils import handle_error
from tiled.utils import DictView, OneShotCachedMap, node_repr

from ._common import IPYTHON_METHODS
from .document import DatumPage, Descriptor, EventPage, Resource, Start, Stop

_document_types = {
    "start": Start,
    "stop": Stop,
    "descriptor": Descriptor,
    "event_page": EventPage,
    "datum_page": DatumPage,
    "resource": Resource,
    "stream_resource": None,
    "stream_datum": None,
}

RESERVED_KEYS = {"streams", "views", "config", "auxiliary"}


class BlueskyRun(Container):
    """
    This encapsulates the data and metadata for one Bluesky 'run'.

    This adds for bluesky-specific conveniences to the standard client Container.
    """

    def __repr__(self):
        metadata = self.metadata
        datetime_ = datetime.fromtimestamp(metadata["start"]["time"])
        return (
            f"<{type(self).__name__} "
            f"{set(self)!r} "
            f"scan_id={metadata['start'].get('scan_id', 'UNSET')!s} "  # (scan_id is optional in the schema)
            f"uid={metadata['start']['uid'][:8]!r} "  # truncated uid
            f"{datetime_.isoformat(sep=' ', timespec='minutes')}"
            ">"
        )

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

    @property
    def v2(self):
        return self

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
        link = self.item["links"]["self"].replace("/metadata", "/documents", 1)
        with self.context.http_client.stream(
            "GET",
            link,
            params={"fill": fill},
            headers={"Accept": "application/json-seq"},
        ) as response:
            if response.is_error:
                response.read()
                handle_error(response)
            tail = ""
            for chunk in response.iter_bytes():
                for line in chunk.decode().splitlines(keepends=True):
                    if line[-1] == "\n":
                        item = json.loads(tail + line)
                        yield (item["name"], _document_types[item["name"]](item["doc"]))
                        tail = ""
                    else:
                        tail += line
            if tail:
                item = json.loads(tail)
                yield (item["name"], _document_types[item["name"]](item["doc"]))

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

    def describe(self):
        "For back-compat with intake-based BlueskyRun"
        warnings.warn(
            "This will be removed. Use .metadata directly instead of describe()['metadata'].",
            DeprecationWarning,
            stacklevel=2,
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
            "Reading any entire run is not supported. Access a stream in this run and read that."
        )

    to_dask = read


class BlueskyRunV2(Container):
    """
    This encapsulates the data and metadata for one Bluesky 'run'.

    This adds for bluesky-specific conveniences to the standard client Container.
    """

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self._stream_names = set(super().get("streams", ()))

    def __repr__(self):
        metadata = self.metadata
        datetime_ = datetime.fromtimestamp(metadata["start"]["time"])
        return (
            f"<{type(self).__name__} "
            f"{self._stream_names!r} "
            f"scan_id={metadata['start'].get('scan_id', 'UNSET')!s} "  # (scan_id is optional in the schema)
            f"uid={metadata['start']['uid'][:8]!r} "  # truncated uid
            f"{datetime_.isoformat(sep=' ', timespec='minutes')}"
            ">"
        )

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

    @property
    def v2(self):
        return self

    def documents(self, fill=False):
        pass
        # # For back-compat with v2:
        # if fill == "yes":
        #     fill = True
        # elif fill == "no":
        #     fill = False
        # elif fill == "delayed":
        #     raise NotImplementedError("fill='delayed' is not supported")
        # else:
        #     fill = bool(fill)
        # link = self.item["links"]["self"].replace("/metadata", "/documents", 1)
        # with self.context.http_client.stream(
        #     "GET",
        #     link,
        #     params={"fill": fill},
        #     headers={"Accept": "application/json-seq"},
        # ) as response:
        #     if response.is_error:
        #         response.read()
        #         handle_error(response)
        #     tail = ""
        #     for chunk in response.iter_bytes():
        #         for line in chunk.decode().splitlines(keepends=True):
        #             if line[-1] == "\n":
        #                 item = json.loads(tail + line)
        #                 yield (item["name"], _document_types[item["name"]](item["doc"]))
        #                 tail = ""
        #             else:
        #                 tail += line
        #     if tail:
        #         item = json.loads(tail)
        #         yield (item["name"], _document_types[item["name"]](item["doc"]))

    def __getitem__(self, key):
        # Process reserved keys:
        if key in RESERVED_KEYS:
            return super().__getitem__(key)

        if key in self._stream_names:
            stream_container = super().get("streams", {}).get(key)
            stream_config = super().get("config", {}).get(key)
            return BlueskyStreamView.from_container_and_config(stream_container, stream_config)

        if "/" in key:
            key, rest = key.split("/", 1)
            return self[key][rest]

        return super().__getitem__(key)

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

    def describe(self):
        "For back-compat with intake-based BlueskyRun"
        warnings.warn(
            "This will be removed. Use .metadata directly instead of describe()['metadata'].",
            DeprecationWarning,
            stacklevel=2,
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
            "Reading any entire run is not supported. Access a stream in this run and read that."
        )

    to_dask = read


class VirtualContainer(DictView):
    def __repr__(self):
        tiled_repr = node_repr(self, self._internal_dict.keys())
        return tiled_repr.replace(type(self).__name__, "ContainerClient")

    def __getitem__(self, key):
        if "/" in key:
            key, rest = key.split("/", 1)
            return self[key][rest]

        return super().__getitem__(key)


class VirtualDatasetClient(DictView):
    def __repr__(self):
        tiled_repr = node_repr(self, self._internal_dict.keys())
        return tiled_repr.replace(type(self).__name__, "DatasetClient")

    def read(self):
        d = {k: {"dims": "time", "data": v.read()} for k, v in self._internal_dict.items()}
        return xarray.Dataset.from_dict(d)


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


class BlueskyStreamView(OneShotCachedMap):
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
        result = {k: VirtualDatasetClient(v) for k, v in values.items()}
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
            "data": lambda: stream_client.to_dataset(*sorted(set(data_keys))),
            "timestamps": lambda: stream_client.to_dataset(*ts_keys),
            "config": lambda: cls.format_config(config_client),
            "config_timestamps": lambda: cls.format_config(config_client, timestamp=True),
        }

        # Construct the metadata
        metadata = {"descriptors": [], "stream_name": stream_client.item["id"], **stream_client.metadata}

        return cls(internal_dict, metadata=metadata)
