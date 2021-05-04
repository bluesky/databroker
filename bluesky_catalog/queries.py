import collections.abc
from dataclasses import dataclass
import enum
import json
from typing import List, Optional

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
        if timezone is None:
            import tzlocal

            timezone = tzlocal.get_localzone().zone
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
