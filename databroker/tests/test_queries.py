import pickle

from bluesky.plans import count
import pytest

from tiled.queries import (
    Contains,
    FullText,
    In,
    Key,
    NotIn,
    Regex,
)
from databroker.queries import (
    ScanID,
    ScanIDRange,
    TimeRange,
)
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

    for key in ["foo", "start.foo"]:
        results = c.search(Regex(key, "a[1-2]"))
        assert should_match1 in results
        assert should_match2 in results
        assert should_not_match not in results


def test_eq(c, RE, hw):
    RE.subscribe(c.v1.insert)

    (should_match,) = get_uids(RE(count([hw.det]), foo="a"))
    (should_not_match,) = get_uids(RE(count([hw.det]), foo="b"))

    for key in ["foo", "start.foo"]:
        results = c.search(Key(key) == "a")
        assert should_match in results
        assert should_not_match not in results


def test_not_eq(c, RE, hw):
    RE.subscribe(c.v1.insert)

    (should_match,) = get_uids(RE(count([hw.det]), foo="a"))
    (should_not_match,) = get_uids(RE(count([hw.det]), foo="b"))

    for key in ["foo", "start.foo"]:
        results = c.search(Key(key) != "b")
        assert should_match in results
        assert should_not_match not in results


def test_scan_id(c, RE, hw):
    RE.subscribe(c.v1.insert)

    (should_match,) = get_uids(RE(count([hw.det])))
    (should_not_match,) = get_uids(RE(count([hw.det])))

    scan_id = c[should_match].start["scan_id"]
    results = c.search(ScanID(scan_id))

    assert scan_id == results[0].start["scan_id"]


def test_scan_id_range(c, RE, hw):
    RE.subscribe(c.v1.insert)

    (scan1,) = get_uids(RE(count([hw.det])))
    scan_id1 = c[scan1].start["scan_id"]
    (scan2,) = get_uids(RE(count([hw.det])))
    scan_id2 = c[scan2].start["scan_id"]
    (scan3,) = get_uids(RE(count([hw.det])))
    scan_id3 = c[scan3].start["scan_id"]

    results = c.search(ScanIDRange(scan_id1, scan_id3))
    scan_id_results = [run.start["scan_id"] for uid, run in results.items()]
    assert scan_id_results == [scan_id1, scan_id2]
    assert scan_id3 not in scan_id_results


def test_in(c, RE, hw):
    RE.subscribe(c.v1.insert)

    (should_match,) = get_uids(RE(count([hw.det]), foo="a"))
    (should_not_match,) = get_uids(RE(count([hw.det]), foo="b"))

    for key in ["foo", "start.foo"]:
        results = c.search(In(key, ["a", "z"]))
        assert should_match in results
        assert should_not_match not in results


def test_not_in(c, RE, hw):
    RE.subscribe(c.v1.insert)

    (should_match,) = get_uids(RE(count([hw.det]), foo="a"))
    (should_not_match,) = get_uids(RE(count([hw.det]), foo="b"))

    for key in ["foo", "start.foo"]:
        results = c.search(NotIn(key, ["b", "z"]))
        assert should_match in results
        assert should_not_match not in results


def test_comparison(c, RE, hw):
    RE.subscribe(c.v1.insert)

    (should_match,) = get_uids(RE(count([hw.det]), foo=5))
    (should_not_match,) = get_uids(RE(count([hw.det]), foo=15))

    for key in ["foo", "start.foo"]:
        results = c.search(Key(key) < 10)
        assert should_match in results
        assert should_not_match not in results


def test_contains(c, RE, hw):
    RE.subscribe(c.v1.insert)

    (should_match,) = get_uids(RE(count([hw.det]), foo=[1, 3, 5, 7, 9]))
    (should_not_match,) = get_uids(RE(count([hw.det]), foo=[2, 4, 6, 8, 10]))

    for key in ["foo", "start.foo"]:
        results = c.search(Contains(key, 3))
        assert should_match in results
        assert should_not_match not in results


def test_distinct(c, RE, hw):
    RE.subscribe(c.v1.insert)

    (should_match,) = get_uids(RE(count([hw.det]), foo="a"))
    (should_not_match,) = get_uids(RE(count([hw.det]), foo="b"))

    expected = {
        "metadata": {
            "start.foo": [{"value": "a", "count": 1}, {"value": "b", "count": 1}]
        },
        "structure_families": [{"value": "container", "count": 2}],
        "specs": [{"value": [{"name": "BlueskyRun", "version": "1"}], "count": 2}],
    }

    results = c.distinct("foo", structure_families=True, specs=True, counts=True)

    assert results["metadata"] == expected["metadata"]
    assert results["specs"] == expected["specs"]
    assert results["structure_families"] == expected["structure_families"]
