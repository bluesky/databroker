"""
This module is experimental.
"""
import abc
import collections.abc

import tzlocal

from .utils import normalize_human_friendly_time


class Query(collections.abc.Mapping):
    """
    This represents a MongoDB query.
    
    MongoDB queries are typically encoded as simple dicts. This object supports
    the dict interface in a read-only fashion. Subclassses add a nice __repr__
    and mutable attributes from which the contents of the dict are derived.
    """
    @abc.abstractproperty
    def _query(self):
        ...

    def __iter__(self):
        return iter(self._query)

    def __getitem__(self, key):
        return self._query[key]
    
    def __len__(self):
        return len(self._query)


class TimeRange(Query):
    """
    A search query representing a time range.

    Parameters
    ----------
    since, until: dates gives as timestamp, datetime, or human-friendly string, optional
    timezone : string
        As in, 'US/Eastern'. If None is given, tzlocal is used.
    """
    def __init__(self, since=None, until=None, timezone=None):
        if timezone is None:
            timezone = tzlocal.get_localzone().zone
        self.timezone = timezone
        self.since = since
        self.until = until

    @property
    def since(self):
        return self._since_raw

    @since.setter
    def since(self, value):
        if value is None:
            self._since_normalized = None
        else:
            self._since_normalized = normalize_human_friendly_time(
                value, tz=self.timezone)
        self._since_raw = value

    @property
    def until(self):
        return self._until_raw

    @until.setter
    def until(self, value):
        if value is None:
            self._until_normalized = None
        else:
            self._until_normalized = normalize_human_friendly_time(
                value, tz=self.timezone)
        self._until_raw = value

    @property
    def _query(self):
        query = {'time': {}}
        if self.since is not None:
            query['time']['$gte'] = self._since_normalized
        if self.until is not None:
            query['time']['$lt'] = self._until_normalized
        return query

    def __repr__(self):
        return f"{type(self).__name__}(since={self.since}, until={self.until})"
