from dataclasses import dataclass
import enum
from typing import List, Number

from tiled.query_registration import register


class Duplicates(str, enum.Enum):
    latest = "latest"
    all = "all"
    error = "error"


@register(name="scan_id")
@dataclass
class ScanID:
    """
    Find matches to scan_id(s).
    """

    scan_ids: List[int]
    duplicates: Duplicates

    def __init__(self, *scan_ids, duplicates="latest"):
        self.scan_ids = scan_ids
        self.duplicates = duplicates


@register(name="partial_uid")
@dataclass
class PartialUID:
    """
    Find matches to (partial) uid(s).
    """

    partial_uids: List[str]

    def __init__(self, *partial_uids):
        self.partial_uids = partial_uids


@register(name="duration")
@dataclass
class Duration:
    """
    Run a MongoDB query against a given collection.
    """

    less_than: Number
    greater_than: Number
