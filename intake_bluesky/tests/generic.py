import event_model
import itertools
from intake.catalog.utils import RemoteCatalogError
import numpy
import ophyd.sim
import pytest


def test_fixture(bundle):
    "Simply open the Catalog created by the fixture."


def test_search(bundle):
    "Test search and progressive (nested) search with Mongo queries."
    cat = bundle.cat
    # Make sure the Catalog is nonempty.
    assert list(cat['xyz']())
    # Null search should return full Catalog.
    assert list(cat['xyz']()) == list(cat['xyz'].search({}))
    # Progressive (i.e. nested) search:
    name, = (cat['xyz']
             .search({'plan_name': 'scan'})
             .search({'time': {'$gt': 0}}))
    assert name == bundle.uid


def test_repr(bundle):
    "Test that custom repr (with run uid) appears."
    print(bundle.uid)
    entry = bundle.cat['xyz']()[bundle.uid]
    assert bundle.uid in repr(entry)
    run = entry()
    print(repr(run))
    assert bundle.uid in repr(run)
    assert 'primary' in repr(run)


def test_iteration(bundle):
    cat = bundle.cat['xyz']()
    list(cat)


def test_len(bundle):
    """
    Test that Catalog implements __len__.

    Otherwise intake will loop it as `sum(1 for _ in catalog)` which is likely
    less efficient.
    """
    cat = bundle.cat['xyz']()
    len(cat)  # If not implemented, will raise TypeError


def test_getitem_sugar(bundle):
    cat = bundle.cat['xyz']()

    # Test lookup by recency (e.g. -1 is latest)
    cat[-1]
    with pytest.raises((IndexError, RemoteCatalogError)):
        cat[-(1 + len(cat))]  # There aren't this many entries

    # Test lookup by integer, not globally-unique, 'scan_id'.
    expected = cat[bundle.uid]()
    scan_id = expected.metadata['start']['scan_id']
    actual = cat[scan_id]()
    assert actual.metadata['start']['uid'] == expected.metadata['start']['uid']
    with pytest.raises((KeyError, RemoteCatalogError)):
        cat[234234234234234234]  # This scan_id doesn't exit.

    # Test lookup by partial uid.
    expected = cat[bundle.uid]()
    uid = bundle.uid
    for j in itertools.count(8, len(uid)):
        trunc_uid = uid[:j]
        try:
            int(trunc_uid)
        except ValueError:
            break
        else:
            continue
    else:
        raise pytest.skip(
            "got an all int (!?) uid, can not truncate and retrieve "
            "due to intake not respecting types in getitem across the network.")
    actual = cat[trunc_uid]()
    assert actual.metadata['start']['uid'] == expected.metadata['start']['uid']


def test_run_read_not_implemented(bundle):
    "Test that custom repr (with run uid) appears."
    run = bundle.cat['xyz']()[bundle.uid]
    with pytest.raises(NotImplementedError):
        run.read()
    with pytest.raises(NotImplementedError):
        run.to_dask()


def test_run_metadata(bundle):
    "Find 'start' and 'stop' in the Entry metadata."
    run = bundle.cat['xyz']()[bundle.uid]
    for key in ('start', 'stop'):
        assert key in run.metadata  # entry
        assert key in run().metadata  # datasource


def test_read_canonical(bundle):
    run = bundle.cat['xyz']()[bundle.uid]
    run.read_canonical()
    filler = event_model.Filler({'NPY_SEQ': ophyd.sim.NumpySeqHandler})

    def sorted_actual():
        for name_ in ('start', 'descriptor', 'resource', 'datum', 'event_page', 'event', 'stop'):
            for name, doc in bundle.docs:
                # Fill external data.
                _, filled_doc = filler(name, doc)
                if name == name_ and name in ('start', 'descriptor', 'event', 'event_page', 'stop'):
                    yield name, filled_doc

    for actual, expected in itertools.zip_longest(
            run.read_canonical(), sorted_actual()):
        actual_name, actual_doc = actual
        expected_name, expected_doc = expected
        print(expected_name)
        try:
            assert actual_name == expected_name
        except ValueError:
            assert numpy.array_equal(actual_doc, expected_doc)


def test_read(bundle):
    run = bundle.cat['xyz']()[bundle.uid]()
    entry = run['primary']
    entry.read()
    entry().to_dask()
    entry().to_dask().load()


def test_include_and_exclude(bundle):
    run = bundle.cat['xyz']()[bundle.uid]()
    entry = run['primary']
    assert 'motor' in entry().read().variables
    assert 'motor' not in entry(exclude=['motor']).read().variables
    assert 'motor' in entry(exclude=['NONEXISTENT']).read().variables
    expected = set(['time', 'uid', 'seq_num', 'motor'])
    assert set(entry(include=['motor']).read().variables) == expected
    expected = set(['time', 'uid', 'seq_num', 'motor:motor_velocity'])
    assert set(entry(include=['motor:motor_velocity']).read().variables) == expected
