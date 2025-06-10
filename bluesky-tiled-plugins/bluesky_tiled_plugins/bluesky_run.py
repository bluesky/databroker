import copy
import functools
import io
import json
import keyword
import warnings
from datetime import datetime
from typing import Optional

from tiled.client.container import Container
from tiled.client.utils import handle_error

from ._common import IPYTHON_METHODS
from .bluesky_event_stream import BlueskyEventStreamV2SQL
from .document import DatumPage, Descriptor, Event, EventPage, Resource, Start, Stop, StreamDatum, StreamResource

_document_types = {
    "start": Start,
    "stop": Stop,
    "event": Event,
    "descriptor": Descriptor,
    "event_page": EventPage,
    "datum_page": DatumPage,
    "resource": Resource,
    "stream_resource": StreamDatum,
    "stream_datum": StreamResource,
}

RESERVED_V3_KEYS = {"configs", "streams", "views", "aux"}


class BlueskyRun(Container):
    _ipython_display_ = None
    _repr_mimebundle_ = None

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

    @functools.cached_property
    def descriptors(self):
        return [doc for name, doc in self.documents() if name == "descriptor"]

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
    """A MongoDB-native layout of BlueskyRuns

    This layout has been in use prior to the introduction of SQL backend in May 2025.
    """

    _version = "2.0"

    def __new__(cls, context, *, item, structure_clients, **kwargs):
        # When inheriting, return the class itself
        if cls is not BlueskyRunV2:
            return super().__new__(cls, context, item=item, structure_clients=structure_clients, **kwargs)

        _cls = BlueskyRunV2SQL if cls._is_sql(item) else BlueskyRunV2Mongo
        return _cls(context, item=item, structure_clients=structure_clients, **kwargs)

    @property
    def v1(self):
        "Accessor to legacy interface."
        from databroker.v1 import Broker, Header

        db = Broker(self)
        header = Header(self, db)
        return header

    @property
    def v2(self):
        return self

    @property
    def v3(self):
        if not self._is_sql(self.item):
            raise NotImplementedError("v3 is not available for MongoDB-based BlueskyRun")

        structure_clients = copy.copy(self.structure_clients)
        structure_clients.set("BlueskyRun", lambda: BlueskyRunV3)
        return BlueskyRunV3(self.context, item=self.item, structure_clients=structure_clients)


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


class _BlueskyRunSQL(BlueskyRun):
    """A base class for a BlueskyRun that is backed by a SQL database.

    This class implements the SQL-specific method for accessing the stream of
    Bluesky documents. It is not intended to be used directly, but rather as a
    base class for other classes (v2 and v3) that implement additional methods.
    """

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

    @functools.cached_property
    def _stream_names(self):
        return sorted(self.get("streams", ()))

    def documents(self, fill=False):
        with io.BytesIO() as buffer:
            self.export(buffer, format="application/json-seq")
            buffer.seek(0)
            for line in buffer:
                parsed = json.loads(line.decode().strip())
                yield parsed["name"], _document_types[parsed["name"]](parsed["doc"])


class BlueskyRunV2SQL(BlueskyRunV2, _BlueskyRunSQL):
    def _keys_slice(self, start, stop, direction, page_size: Optional[int] = None, **kwargs):
        keys = reversed(self._stream_names) if direction < 0 else self._stream_names
        return (yield from keys[start:stop])

    def _items_slice(self, start, stop, direction, page_size: Optional[int] = None, **kwargs):
        _streams_node = super().get("streams", {})
        for key in reversed(self._stream_names) if direction < 0 else self._stream_names:
            yield key, _streams_node.get(key)
        return

    def __getitem__(self, key):
        # For v3, we need to handle the streams and configs keys
        if key in RESERVED_V3_KEYS:
            return super().__getitem__(key)

        if key in self._stream_names:
            stream_container = super().get("streams", {}).get(key)
            return BlueskyEventStreamV2SQL.from_stream_client(stream_container)

        if "/" in key:
            key, rest = key.split("/", 1)
            return self[key][rest]

        return super().__getitem__(key)

    def __iter__(self):
        yield from self._stream_names


class BlueskyRunV3(_BlueskyRunSQL):
    """A BlueskyRun that is backed by a SQL database."""

    _version = "3.0"

    def __new__(cls, context, *, item, structure_clients, **kwargs):
        # When inheriting, return the class itself
        if cls is not BlueskyRunV3 or cls._is_sql(item):
            return super().__new__(cls, context, item=item, structure_clients=structure_clients, **kwargs)
        else:
            return BlueskyRunV2Mongo(context, item=item, structure_clients=structure_clients, **kwargs)

    def __getattr__(self, key):
        if key in self._stream_names:
            # A shortcut to the stream data
            return self["streams"][key]

        return super().__getattr__(key)

    def __repr__(self):
        metadata = self.metadata
        datetime_ = datetime.fromtimestamp(metadata["start"]["time"])
        return (
            f"<BlueskyRun v{self._version} "
            f"streams: {set(self._stream_names) or 'NONE'} "
            f"scan_id={metadata['start'].get('scan_id', 'UNSET')!s} "  # (scan_id is optional in the schema)
            f"uid={metadata['start']['uid'][:8]!r} "  # truncated uid
            f"{datetime_.isoformat(sep=' ', timespec='minutes')}"
            ">"
        )

    @property
    def v1(self):
        "Access to legacy interface"
        return self.v2.v1

    @property
    def v2(self):
        structure_clients = copy.copy(self.structure_clients)
        structure_clients.set("BlueskyRun", lambda: BlueskyRunV2)
        return BlueskyRunV2(self.context, item=self.item, structure_clients=structure_clients)

    @property
    def v3(self):
        return self
