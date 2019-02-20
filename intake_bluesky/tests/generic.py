import event_model
import itertools
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
    # Null serach should return full Catalog.
    assert list(cat['xyz']()) == list(cat['xyz'].search({}))
    # Progressive (i.e. nested) search:
    name, = (cat['xyz']
             .search({'plan_name': 'scan'})
             .search({'time': {'$gt': 0}}))
    assert name == bundle.uid


def test_repr(bundle):
    "Test that custom repr (with run uid) appears."
    run = bundle.cat['xyz']()[bundle.uid]
    assert bundle.uid in repr(run)


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
