import collections.abc
from dataclasses import dataclass
import enum
import json
from typing import List

from tiled.query_registration import register


class Duplicates(str, enum.Enum):
    latest = "latest"
    all = "all"
    error = "error"


@register(name="scan_id")
@dataclass
class _ScanID:
    """
    Find matches to scan_id(s).
    """

    scan_ids: List[int]
    duplicates: Duplicates


def ScanID(*scan_ids, duplicates="latest"):
    # Wrap _ScanID to provide a nice usage for *one or more scan_ids*:
    # >>> ScanID(5)
    # >>> ScanID(5, 6, 7)
    # Placing a varargs parameter (*scan_ids) in the dataclass constructor
    # would cause trouble on the server side and generally feels "wrong"
    # so we have this wrapper function instead.
    return _ScanID(scan_ids=scan_ids, duplicates=duplicates)


@register(name="partial_uid")
@dataclass
class _PartialUID:
    """
    Find matches to (partial) uid(s).
    """

    partial_uids: List[str]


def PartialUID(*partial_uids):
    # See comment above with ScanID and _ScanID. Same thinking here.
    return _PartialUID(partial_uids)


@register(name="duration")
@dataclass
class Duration:
    """
    Run a MongoDB query against a given collection.
    """

    less_than: float
    greater_than: float


@register(name="raw_mongo")
@dataclass
class RawMongo:
    """
    Run a MongoDB query against a given collection.
    """

    start: str  # We cannot put a dict in a URL, so this a JSON str.

    def __init__(self, *, start):
        if isinstance(start, collections.abc.Mapping):
            start = json.dumps(start)
        self.start = start
