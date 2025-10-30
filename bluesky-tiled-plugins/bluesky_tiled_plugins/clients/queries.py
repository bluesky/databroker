"""
This module defines objects designed to make queries on
CatalogOfBlueskyRuns convenient.

Older clients used these query object to issue custom query types.
This requires servers to register custom implementations of those
query types.

Newer clients use these object as pure client-side conveniences. In
`CatalogOfBlueskyRuns.search` method, they are decomposed into standard Tiled
queries, requiring no custom counterpart on the server.

The registration and serialization aspects are (temporarily) retained in order
to support older clients querying against MongoDB-backed servers.
"""

import enum
import warnings
from dataclasses import asdict, dataclass
from typing import Optional

# Not all of these are used, but import them all
# for user convenience so everything can be imported from bluesky_tiled_plugins.queries
from tiled.queries import (  # noqa: F401
    Comparison,
    Contains,
    Eq,
    FullText,
    In,
    Key,
    NotEq,
    NotIn,
    Operator,
    QueryValueError,
    Regex,
)
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

    scan_ids: list[int]
    duplicates: Duplicates

    def __init__(self, *, scan_ids, duplicates):
        self.scan_ids = scan_ids
        self.duplicates = Duplicates(duplicates)

    def encode(self):
        return {
            "scan_ids": ",".join(str(scan_id) for scan_id in self.scan_ids),
            "duplicates": self.duplicates.value,
        }

    @classmethod
    def decode(cls, *, scan_ids, duplicates):
        return cls(
            scan_ids=[int(scan_id) for scan_id in scan_ids.split(",")],
            duplicates=Duplicates(duplicates),
        )


def ScanID(*scan_ids, duplicates="latest"):
    # Wrap _ScanID to provide a nice usage for *one or more scan_ids*:
    # >>> ScanID(5)
    # >>> ScanID(5, 6, 7)
    # Placing a varargs parameter (*scan_ids) in the dataclass constructor
    # would cause trouble on the server side and generally feels "wrong"
    # so we have this wrapper function instead.
    return _ScanID(scan_ids=scan_ids, duplicates=duplicates)


@register(name="scan_id_range")
@dataclass
class ScanIDRange:
    """
    Find scans in the range.
    """

    start_id: int
    end_id: int
    duplicates: Duplicates

    def __init__(self, start_id, end_id, duplicates="latest"):
        self.start_id = start_id
        self.end_id = end_id
        self.duplicates = Duplicates(duplicates)

    def encode(self):
        return {
            "start_id": self.start_id,
            "end_id": self.end_id,
            "duplicates": self.duplicates.value,
        }

    @classmethod
    def decode(cls, *, start_id, end_id, duplicates="latest"):
        return cls(
            start_id=int(start_id),
            end_id=int(end_id),
            duplicates=Duplicates(duplicates),
        )


@register(name="partial_uid")
@dataclass
class _PartialUID:
    """
    Find matches to (partial) uid(s).
    """

    partial_uids: list[str]

    def encode(self):
        return {"partial_uids": ",".join(str(uid) for uid in self.partial_uids)}

    @classmethod
    def decode(cls, *, partial_uids):
        return cls(partial_uids=partial_uids.split(","))


def PartialUID(*partial_uids):
    # See comment above with ScanID and _ScanID. Same thinking here.
    return _PartialUID(partial_uids)


def RawMongo(start):
    """
    DEPRECATED

    Raw MongoDB queries are no longer supported.  If it is possible to express
    the import as a supported query, we transform it and warn. If not, we raise
    an error.
    """

    if len(start) == 1:
        ((key, value),) = start.items()
        if not isinstance(value, dict):
            # We can transform this into a simple query.
            warnings.warn(
                """RawMongo will not be supported
in a future release of databroker, and its functionality has been limited.
Instead, use:

    Key("{key}") == {value!r}
""",
                stacklevel=2,
            )
            return Key(key) == value
    raise ValueError(
        """Arbitrary MongoDB queries no longer supported.

If this is critical to you, please open an issue at

    https://github.com/bluesky/databroker

describing your use case and we will see what we can work out."""
    )


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

    from datetime import datetime

    import pytz

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
                raise TypeError(f"expected datetime, got {ts:r}")

        except NameError:
            raise ValueError("failed to parse time: " + repr(val)) from None

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

    def encode(self):
        return asdict(self)

    @classmethod
    def decode(cls, *, timezone, since=None, until=None):
        return cls(timezone=timezone, since=since, until=until)
