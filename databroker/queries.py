import collections.abc
from dataclasses import dataclass
import enum
import json
from typing import List, Optional

from tiled.adapters.mapping import MapAdapter, full_text_search
from tiled.queries import FullText, QueryValueError
from tiled.query_registration import QueryTranslationRegistry

# Reimport generic queries for convenience so all can be imported from this module.
from tiled.query_registration import register

from .common import CatalogOfBlueskyRunsMixin


class BlueskyMapAdapter(MapAdapter, CatalogOfBlueskyRunsMixin):
    """
    A Tree that contains BlueskyRuns and supports relevant queries on them.
    """

    # The primary purpose of this class is to have a query_registry
    # distinct form the generic tiled.in_memory.Tree.query_registry
    # with queries that assume the contents are BlueskyRuns and have the
    # requisite metadata structure.
    query_registry = QueryTranslationRegistry()
    register_query = query_registry.register
    register_query_lazy = query_registry.register_lazy


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


# human friendly timestamp formats we'll parse
_TS_FORMATS = [
    "%Y-%m-%d %H:%M:%S",
    "%Y-%m-%d %H:%M",  # these 2 are not as originally doc'd,
    "%Y-%m-%d %H",  # but match previous pandas behavior
    "%Y-%m-%d",
    "%Y-%m",
    "%Y",
]


def _normalize_human_friendly_time(val, tz):
    """Given one of :
    - string (in one of the formats below)
    - datetime (eg. datetime.now()), with or without tzinfo)
    - timestamp (eg. time.time())
    return a timestamp (seconds since jan 1 1970 UTC).

    Non string/datetime values are returned unaltered.
    Leading/trailing whitespace is stripped.
    Supported formats:
    {}
    """
    # {} is placeholder for formats; filled in after def...

    import pytz
    from datetime import datetime

    zone = pytz.timezone(tz)  # tz as datetime.tzinfo object
    epoch = pytz.UTC.localize(datetime(1970, 1, 1))
    check = True

    if isinstance(val, str):
        # unix 'date' cmd format '%a %b %d %H:%M:%S %Z %Y' works but
        # doesn't get TZ?

        # Could cleanup input a bit? remove leading/trailing [ :,-]?
        # Yes, leading/trailing whitespace to match pandas behavior...
        # Actually, pandas doesn't ignore trailing space, it assumes
        # the *current* month/day if they're missing and there's
        # trailing space, or the month is a single, non zero-padded digit.?!
        val = val.strip()

        for fmt in _TS_FORMATS:
            try:
                ts = datetime.strptime(val, fmt)
                break
            except ValueError:
                pass

        try:
            if isinstance(ts, datetime):
                val = ts
                check = False
            else:
                # what else could the type be here?
                raise TypeError("expected datetime," " got {:r}".format(ts))

        except NameError:
            raise ValueError("failed to parse time: " + repr(val))

    if check and not isinstance(val, datetime):
        return val

    if val.tzinfo is None:
        # is_dst=None raises NonExistent and Ambiguous TimeErrors
        # when appropriate, same as pandas
        val = zone.localize(val, is_dst=None)

    return (val - epoch).total_seconds()


@register(name="time_range")
@dataclass
class TimeRange:
    """
    A search query representing a time range.

    Parameters
    ----------
    since, until: dates gives as timestamp, datetime, or human-friendly string, optional
    timezone : string
        As in, 'US/Eastern'. If None is given, tzlocal is used.

    Examples
    --------
    Any granularity (year, month, date, hour, minute, second) is accepted.

    >>> TimeRange(since='2014')

    >>> TimeRange(until='2019-07')

    >>> TimeRange(since='2014-07-04', until='2020-07-04')

    >>> TimeRange(since='2014-07-04 05:00')

    """

    timezone: str
    since: Optional[float] = None
    until: Optional[float] = None

    def __init__(self, *, timezone=None, since=None, until=None):
        # Stash the raw values just for use in the repr.
        self._raw_since = since
        self._raw_until = until

        if timezone is None:
            import tzlocal

            lz = tzlocal.get_localzone()
            try:
                timezone = lz.key
            except AttributeError:
                timezone = lz.zone
        self.timezone = timezone
        if since is None:
            self.since = None
        else:
            self.since = _normalize_human_friendly_time(since, tz=self.timezone)
        if until is None:
            self.until = None
        else:
            self.until = _normalize_human_friendly_time(until, tz=self.timezone)
        if since is not None and until is not None:
            if self.since > self.until:
                raise ValueError("since must not be greater than until.")

    def __repr__(self):
        return (
            f"{type(self).__name__!s}("
            f"timezone={self.timezone!r}, since={self._raw_since!r}, until={self._raw_until!r})"
        )


def raw_mongo_in_memory(query, catalog):

    from mongoquery import Query

    query_obj = Query(json.loads(query.start))
    matches = {
        key: value
        for key, value in catalog.items()
        if query_obj.match(value.metadata["start"])
    }
    return catalog.new_variation(mapping=matches)


def scan_id(query, catalog):
    mongo_results = catalog.query_registry(
        RawMongo(start={"scan_id": {"$in": query.scan_ids}}),
        catalog,
    )
    # Handle duplicates.
    if query.duplicates == "latest":
        # Convert to a BlueskyMapAdapter to do some filtering in Python
        # that we cannot expressing in a collection.find(...) query.
        # We might want to rethink this later and make it possible to do
        # aggregations in Mongo from queries.
        results_by_scan_id = {}
        for key, value in mongo_results.items():
            results_by_scan_id[value.metadata["start"]["scan_id"]] = (key, value)
        results = BlueskyMapAdapter(dict(results_by_scan_id.values()), must_revalidate=False)
    elif query.duplicates == "error":
        scan_ids = list(
            value.metadata["start"]["scan_id"] for value in mongo_results.values()
        )
        counter = collections.Counter(scan_ids)
        duplicated = []
        for k, v in counter.items():
            if v > 1:
                duplicated.append(k)
        if duplicated:
            raise QueryValueError(
                f"There are multiples of the following scan_ids: {duplicated}"
            )
        results = mongo_results
    elif query.duplicates == "all":
        results = mongo_results
    else:
        raise QueryValueError("duplicates should be one of {'latest', 'error', 'all'}")
    return results


def partial_uid(query, catalog):
    results = {}
    for partial_uid in query.partial_uids:
        if len(partial_uid) < 5:
            raise QueryValueError(
                f"Partial uid {partial_uid} is too short. "
                "It must include at least 5 characters."
            )
        result = catalog.query_registry(
            RawMongo(start={"uid": {"$regex": f"^{partial_uid}"}}), catalog
        )
        if len(result) > 1:
            raise QueryValueError(
                f"Partial uid {partial_uid} has multiple matches, "
                "listed below. Include more characters. Matches:\n" + "\n".join(result)
            )
        results.update(result)
    return BlueskyMapAdapter(results, must_revalidate=False)


def time_range(query, catalog):
    mongo_query = {"time": {}}
    if query.since is not None:
        mongo_query["time"]["$gte"] = query.since
    if query.until is not None:
        mongo_query["time"]["$lt"] = query.until
    if not mongo_query["time"]:
        # Neither 'since' nor 'until' are set.
        mongo_query.clear()
    return catalog.query_registry(RawMongo(start=mongo_query), catalog)


BlueskyMapAdapter.register_query(_PartialUID, partial_uid)
BlueskyMapAdapter.register_query(RawMongo, raw_mongo_in_memory)
BlueskyMapAdapter.register_query(_ScanID, scan_id)
BlueskyMapAdapter.register_query(TimeRange, time_range)
BlueskyMapAdapter.register_query(FullText, full_text_search)
