import pickle

import pytest

from ..queries import TimeRange


def test_time_range():
    empty = TimeRange()
    assert empty == dict(empty) == empty.query == {}
    assert TimeRange(**empty.kwargs) == empty

    since = TimeRange(since='2020')
    expected = {'time': {'$gte': 1577854800.0}}
    assert since == dict(since) == since.query == expected
    assert TimeRange(**since.kwargs) == since

    until = TimeRange(until='2020')
    expected = {'time': {'$lt': 1577854800.0}}
    assert until == dict(until) == until.query == expected
    assert TimeRange(**until.kwargs) == until

    both = TimeRange(since='2020', until='2021')
    expected = {'time': {'$gte': 1577854800.0, '$lt': 1609477200.0}}
    assert both == dict(both) == both.query == expected
    assert TimeRange(**both.kwargs) == both

    with pytest.raises(ValueError):
        # since must not be greater than until
        TimeRange(since='2021', until='2020')

    with_tz = TimeRange(since='2020-01-01 9:52', timezone='Europe/Amsterdam')
    expected = {'time': {'$gte': 1577868720.0}}
    assert with_tz == dict(with_tz) == with_tz.query == expected
    assert TimeRange(**with_tz.kwargs) == with_tz


def test_replace():
    "Test the Query.replace() method using TimeRange."
    original = TimeRange(since='2020', until='2021')
    clone = original.replace()
    assert original == clone

    replaced = original.replace(since='1999')
    assert replaced != original


def test_pickle():
    "Ensure that query objects are pickle-able."
    q = TimeRange(since='2020-01-01 9:52')
    serialized = pickle.dumps(q)
    deserialized = pickle.loads(serialized)
    assert q == deserialized
