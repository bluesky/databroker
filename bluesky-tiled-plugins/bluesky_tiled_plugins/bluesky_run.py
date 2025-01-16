import json
import keyword
import warnings
from datetime import datetime

from tiled.client.container import Container
from tiled.client.utils import handle_error

from ._common import IPYTHON_METHODS
from .document import Start, Stop, Descriptor, EventPage, DatumPage, Resource


_document_types = {
    "start": Start,
    "stop": Stop,
    "descriptor": Descriptor,
    "event_page": EventPage,
    "datum_page": DatumPage,
    "resource": Resource,
}


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
