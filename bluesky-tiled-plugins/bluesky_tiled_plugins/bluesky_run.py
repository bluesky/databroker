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

RESERVED_KEYS = {"configs", "streams", "views", "aux"}


class BlueskyRun(Container):
    def __new__(cls, context, *, item, structure_clients, **kwargs):
        # When inheriting from BlueskyRun, return the class itself
        if cls is not BlueskyRun:
            return super().__new__(cls)

        # Set the version based on the specs
        _cls = BlueskyRunV3 if cls._is_sql(item) else BlueskyRunV2Mongo
        return _cls(context, item=item, structure_clients=structure_clients, **kwargs)

    @staticmethod
    def _is_sql(item):
        for spec in item["attributes"]["specs"]:
            if spec["name"] == "BlueskyRun":
                if spec["version"].startswith("3."):
                    return True
                return False

    def __repr__(self):
        metadata = self.metadata
        datetime_ = datetime.fromtimestamp(metadata["start"]["time"])
        return (
            f"<BlueskyRun v{self._version} "
            f"{set(self)!r} "  # show the keys
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


class BlueskyRunV2(BlueskyRun):
    _version = "2.0"

    def __new__(cls, context, *, item, structure_clients, **kwargs):
        # When inheriting, return the class itself
        if cls is not BlueskyRunV2:
            return super().__new__(cls, context, item=item, structure_clients=structure_clients, **kwargs)

        _cls = BlueskyRunV2SQL if cls._is_sql(item) else BlueskyRunV2Mongo
        return _cls(context, item=item, structure_clients=structure_clients, **kwargs)

    @property
    def v2(self):
        return self

    @property
    def v3(self):
        self.structure_clients.set("BlueskyRun", lambda: BlueskyRunV3)
        return BlueskyRunV3(self.context, item=self.item, structure_clients=self.structure_clients)


class BlueskyRunV2Mongo(BlueskyRunV2):
    def documents(self, fill=False):
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


class BlueskyRunV2SQL(BlueskyRunV2):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self._stream_names = sorted(set(super().get("streams", ())))

    def _keys_slice(self, start, stop, direction, **kwargs):
        keys = reversed(self._stream_names) if direction < 0 else self._stream_names
        return (yield from keys[start:stop])

    # def _items_slice(self, start, stop, direction, **kwargs):
    #     pass

    def documents(self):
        # TODO: Emmit in the right time order; Use event_model classes

        yield "start", self.start

        # Generate descriptors
        for desc_name in self._stream_names:
            desc_node = self["streams"][desc_name]
            desc_count = desc_node.metadata.get("desc_count", 1)
            data_keys_names = set(desc_node.keys())

            # Assemble dictionaries of data keys and configuration keys
            data_keys, conf_list = {}, [defaultdict(dict) for _ in range(desc_count)]
            object_keys = defaultdict(list)
            conf_node = self["configs"].get(desc_name)
            for item in conf_node.read().to_list() if conf_node else []:
                data_key = item.pop("data_key")  # Must be present
                desc_indx = item.pop("desc_indx", 0)
                value = item.pop("value", None)
                timestamp = item.pop("timestamp", None)
                if data_key in data_keys_names:
                    # This is a proper data_key for internal or external data
                    data_keys[data_key] = {k: v for k, v in item.items() if v is not None}
                    object_name = item.get("object_name")
                    object_keys[object_name].append(data_key)
                elif value is not None:
                    # This is a configuration data_key
                    object_name = item.pop("object_name")

                    conf_data = conf_list[desc_indx][object_name].get("data", {})
                    conf_timestamps = conf_list[desc_indx][object_name].get("timestamps", {})
                    conf_data_keys = conf_list[desc_indx][object_name].get("data_keys", {})

                    conf_data[data_key] = value
                    conf_timestamps[data_key] = float(timestamp) if timestamp else None
                    conf_data_keys[data_key] = {k: v for k, v in item.items() if v is not None}

                    conf_list[desc_indx][object_name]["data"] = conf_data
                    conf_list[desc_indx][object_name]["timestamps"] = conf_timestamps
                    conf_list[desc_indx][object_name]["data_keys"] = conf_data_keys

            # Fixed part of the descriptor
            desc_doc = desc_node.metadata.get("extra", {})
            desc_doc["name"] = desc_name
            desc_doc["data_keys"] = data_keys
            desc_doc["object_keys"] = object_keys

            # Variable part of the descriptor
            for desc_indx in range(desc_count):
                desc_doc["uid"] = conf_node.metadata["descriptors"][desc_indx]["uid"]
                desc_doc["time"] = float(conf_node.metadata["descriptors"][desc_indx]["time"])
                desc_doc["configuration"] = conf_list[desc_indx]

                yield "descriptor", desc_doc

        # Generate events
        for desc_name in self._stream_names:
            desc_node = self["streams"][desc_name]
            if "internal" in desc_node.parts:
                df = desc_node.parts["internal"].read()
                keys = [k for k in df.columns if k not in {"seq_num", "time"} and not k.startswith("ts_")]
                for _, row in df.iterrows():
                    event_doc = {"seq_num": row["seq_num"], "time": float(row["time"])}
                    event_doc["descriptor"] = desc_node.metadata["uid"]
                    event_doc["data"] = {k: row[k] for k in keys}
                    event_doc["timestamps"] = {k: float(row[f"ts_{k}"]) for k in keys}
                    yield "event", event_doc

        # Generate Stream Resources and Datums
        # TODO: needs thorough testing, incl. cases with multiple hdf5 files
        for desc_name in self._stream_names:
            desc_node = self["streams"][desc_name]
            desc_uid = desc_node.metadata["uid"]
            for data_key in desc_node.parts:
                if data_key == "internal":
                    continue
                sres_uid = f"sr-{desc_uid}-{data_key}"  # can be anything (unique)
                ds = desc_node[data_key].data_sources()[0]
                uri = ds.assets[0].data_uri
                for ast in ds.assets:
                    if ast.parameter in {"data_uris", "data_uri"}:
                        uri = ast.data_uri
                        break
                sres_doc = {
                    "data_key": data_key,
                    "uid": sres_uid,
                    "run_start": self.start["uid"],
                    "mimetype": ds.mimetype,
                    "parameters:": ds.parameters,
                    "uri": uri,
                }
                yield "stream_resource", sres_doc

                sdat_uid = f"sd-{desc_uid}-{data_key}-0"  # can be anything (unique)
                total_shape = ds.structure["shape"]
                datum_shape = desc_node.metadata[data_key]["shape"]

                max_indx = (
                    total_shape[0] // datum_shape[0] - 1
                    if len(total_shape) == len(datum_shape)
                    else total_shape[0] - 1
                )
                sdat_doc = {
                    "uid": sdat_uid,
                    "stream_resource": sres_uid,
                    "descriptor": desc_uid,
                    "indices": {"start": 0, "stop": max_indx},
                    "seq_num": {"start": 1, "stop": max_indx + 1},
                }
                yield "stream_datum", sdat_doc

        yield "stop", self.stop

    def __getitem__(self, key):
        # For v3, we need to handle the streams and configs keys
        if key in RESERVED_KEYS:
            return super().__getitem__(key)

        if key in self._stream_names:
            stream_container = super().get("streams", {}).get(key)
            stream_config = super().get("configs", {}).get(key)
            return BlueskyStreamView.from_container_and_config(stream_container, stream_config)

        if "/" in key:
            key, rest = key.split("/", 1)
            return self[key][rest]

        return super().__getitem__(key)

    def __iter__(self):
        yield from self._stream_names


class BlueskyRunV3(BlueskyRun):
    _version = "3.0"

    def __new__(cls, context, *, item, structure_clients, **kwargs):
        # When inheriting, return the class itself
        if cls is not BlueskyRunV3 or cls._is_sql(item):
            return super().__new__(cls, context, item=item, structure_clients=structure_clients, **kwargs)
        else:
            return BlueskyRunV2Mongo(context, item=item, structure_clients=structure_clients, **kwargs)

    @property
    def v2(self):
        self.structure_clients.set("BlueskyRun", lambda: BlueskyRunV2)
        return BlueskyRunV2(self.context, item=self.item, structure_clients=self.structure_clients)

    @property
    def v3(self):
        return self


class VirtualContainer(DictView):
    def __repr__(self):
        tiled_repr = node_repr(self, self._internal_dict.keys())
        return tiled_repr.replace(type(self).__name__, "ContainerClient")

    def __getitem__(self, key):
        if "/" in key:
            key, rest = key.split("/", 1)
            return self[key][rest]

        return super().__getitem__(key)


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
