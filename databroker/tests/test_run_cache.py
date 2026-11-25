"""
The cache of BlueskyRuns in MongoAdapter is used from several threads at once.

A Tiled server calls the adapter from worker threads. The caches (cachetools)
are not safe for that on their own: even reading one changes it.
"""
import sys
import threading

import pytest
from tiled.queries import Key

from ..mongo_normalized import MongoAdapter


class FakeRun:
    "Stands in for a BlueskyRun, so that these tests need no documents."

    def __init__(self, uid):
        self.uid = uid

    def metadata(self):
        return {"start": {"uid": self.uid}, "stop": {}}


@pytest.fixture
def adapter():
    adapter = MongoAdapter.from_mongomock()
    adapter._build_run = lambda run_start_doc: FakeRun(run_start_doc["uid"])
    return adapter


def test_run_cache_used_from_many_threads(adapter):
    # More runs than the cache holds, so that entries are evicted as well as
    # added and read.
    cache = adapter._cache_of_complete_bluesky_runs
    uids = [f"uid-{i}" for i in range(3 * int(cache.maxsize))]
    errors = []

    def worker(seed):
        i = seed
        for _ in range(300):
            i = (i * 1103515245 + 12345) % 2**31  # pseudorandom, repeatable
            uid = uids[i % len(uids)]
            try:
                run = adapter._get_run({"uid": uid})
                assert run.uid == uid
            except Exception as error:
                errors.append(error)

    threads = [threading.Thread(target=worker, args=(seed,)) for seed in range(1, 9)]
    original = sys.getswitchinterval()
    # Switch between threads very often, so that a race shows up quickly.
    sys.setswitchinterval(1e-6)
    try:
        for thread in threads:
            thread.start()
        for thread in threads:
            thread.join()
    finally:
        sys.setswitchinterval(original)

    assert not errors
    # The cache's own bookkeeping is intact.
    assert len(cache) == cache.currsize <= cache.maxsize


def test_run_is_built_without_holding_the_cache_lock(adapter):
    """
    Building a run queries the database, so other threads must not have to
    wait for it. Two threads that ask for the same uncached run both build
    it, and then share the one that was cached first.
    """
    both_are_building = threading.Barrier(2)
    built = []

    def build_run(run_start_doc):
        # This times out (and raises) unless the other thread gets here too.
        both_are_building.wait(timeout=10)
        run = FakeRun(run_start_doc["uid"])
        built.append(run)
        return run

    adapter._build_run = build_run
    results = []
    errors = []

    def worker():
        try:
            results.append(adapter._get_run({"uid": "a"}))
        except Exception as error:
            errors.append(error)

    threads = [threading.Thread(target=worker) for _ in range(2)]
    for thread in threads:
        thread.start()
    for thread in threads:
        thread.join()

    assert not errors
    assert len(built) == 2
    assert len(results) == 2
    assert results[0] is results[1]
    assert adapter._get_run({"uid": "a"}) is results[0]


def test_variations_share_the_cache_lock(adapter):
    "Searching or sorting gives a new adapter with the same caches, so the same lock."
    for variation in [adapter.search(Key("scan_id") == 1), adapter.sort([("time", -1)])]:
        assert isinstance(variation, MongoAdapter)
        assert variation is not adapter
        assert variation._cache_of_complete_bluesky_runs is adapter._cache_of_complete_bluesky_runs
        assert variation._cache_of_partial_bluesky_runs is adapter._cache_of_partial_bluesky_runs
        assert variation._cache_lock is adapter._cache_lock
