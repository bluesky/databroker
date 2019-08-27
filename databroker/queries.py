"""
This module is experimental.
"""
import collections.abc
import abc


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
    """
    def __init__(self, since=None, until=None):
        self.since = since
        self.until = until

    @property
    def _query(self):
        query = {'time': {}}
        if self.since is not None:
            query['time']['$gte'] = self.since
        if self.until is not None:
            query['time']['$lt'] = self.until
        return query

    def __repr__(self):
        return f"{type(self).__name__}(since={self.since}, until={self.until})"
