import pickle

from bluesky.plans import count
import pytest

from ..queries import Contains, FullText, Key, TimeRange, Regex
from ..tests.utils import get_uids


def test_time_range():
    # TODO Test an actual search.
    # RE does not let us spoof time, so this is not straightforward to do cleanly.
    with pytest.raises(ValueError):
        # since must not be greater than until
        TimeRange(since="2021", until="2020")


def test_pickle():
    "Ensure that query objects are pickle-able."
    q = TimeRange(since="2020-01-01 9:52", timezone="US/Eastern")
    serialized = pickle.dumps(q)
    deserialized = pickle.loads(serialized)
    assert q == deserialized


def test_full_text(c, RE, hw):
    RE.subscribe(c.v1.insert)

    (should_match,) = get_uids(RE(count([hw.det]), foo="some words"))
    (should_not_match,) = get_uids(RE(count([hw.det])))

    results = c.search(FullText("some words"))
    assert should_match in results
    assert should_not_match not in results


def test_regex(c, RE, hw):
    RE.subscribe(c.v1.insert)

    (should_match1,) = get_uids(RE(count([hw.det]), foo="a1"))
    (should_match2,) = get_uids(RE(count([hw.det]), foo="a2"))
    (should_not_match,) = get_uids(RE(count([hw.det]), foo="a3", bar="a1"))

    results = c.search(Regex("foo", "a[1-2]"))
    assert should_match1 in results
    assert should_match2 in results
    assert should_not_match not in results


def test_eq(c, RE, hw):
    RE.subscribe(c.v1.insert)

    (should_match,) = get_uids(RE(count([hw.det]), foo="a"))
    (should_not_match,) = get_uids(RE(count([hw.det]), foo="b"))

    results = c.search(Key("foo") == "a")
    assert should_match in results
    assert should_not_match not in results


def test_comparison(c, RE, hw):
    RE.subscribe(c.v1.insert)

    (should_match,) = get_uids(RE(count([hw.det]), foo=5))
    (should_not_match,) = get_uids(RE(count([hw.det]), foo=15))

    results = c.search(Key("foo") < 10)
    assert should_match in results
    assert should_not_match not in results


def test_contains(c, RE, hw):
    RE.subscribe(c.v1.insert)

    (should_match,) = get_uids(RE(count([hw.det]), foo=[1, 3, 5, 7, 9]))
    (should_not_match,) = get_uids(RE(count([hw.det]), foo=[2, 4, 6, 8, 10]))

    results = c.search(Contains("foo", 3))
    assert should_match in results
    assert should_not_match not in results
