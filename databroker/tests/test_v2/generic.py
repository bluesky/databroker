import collections
import event_model
import itertools
from bluesky.plans import count
from intake.catalog.utils import RemoteCatalogError
import numpy
import ophyd.sim
import os
import pytest
import time
import uuid


def normalize(gen):
    """
    Converted any pages to singles.
    """
    for name, doc in gen:
        if name == 'event_page':
            for event in event_model.unpack_event_page(doc):
                yield 'event', event
        elif name == 'datum_page':
            for datum in event_model.unpack_datum_page(doc):
                yield 'datum', datum
        else:
            yield name, doc


def compare(a, b):
    a = normalize(a)
    b = normalize(b)
    a_indexed = {}
    b_indexed = {}
    for name, doc in a:
        if name == 'resource':
            # Check for an extraneous duplicate key in old documents.
            if 'id' in doc:
                assert doc['id'] == doc['uid']
                doc = doc.copy()
                doc.pop('id')
        if name == 'datum':
            a_indexed[('datum', doc['datum_id'])] = doc
        # v0 yields {'_name": 'RunStop'} if the stop doc is missing; v2 yields None.
        elif name == 'stop' and doc is None or 'uid' not in doc:
            a_indexed[(name, None)] = None
        else:
            a_indexed[(name, doc['uid'])] = doc
    for name, doc in b:
        if name == 'resource':
            # Check for an extraneous duplicate key in old documents.
            if 'id' in doc:
                assert doc['id'] == doc['uid']
                doc = doc.copy()
                doc.pop('id')
        if name == 'datum':
            b_indexed[('datum', doc['datum_id'])] = doc
        # v0 yields {'_name": 'RunStop'} if the stop doc is missing; v2 yields None.
        elif name == 'stop' and doc is None or 'uid' not in doc:
            b_indexed[(name, None)] = None
        else:
            b_indexed[(name, doc['uid'])] = doc
    # Same number of each type of document?
    a_counter = collections.Counter(name for name, uid in a_indexed)
    b_counter = collections.Counter(name for name, uid in b_indexed)
    assert a_counter == b_counter
    # Same uids and names?
    assert set(a_indexed) == set(b_indexed)
    # Now delve into the documents themselves...
    for (name, unique_id), a_doc in a_indexed.items():
        b_doc = b_indexed[name, unique_id]
        # Handle special case if 'stop' is None.
        if name == 'stop' and unique_id is None:
            assert b_doc is None or 'uid' not in b_doc
            continue
        # Same top-level keys?
        assert set(a_doc) == set(b_doc)
        # Same contents?
        try:
            a_doc == b_doc
        except ValueError:
            # We end up here if, for example, the dict contains numpy arrays.
            event_model.sanitize_doc(a_doc) == event_model.sanitize_doc(b_doc)


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
    result = (cat['xyz']
             .search({'plan_name': 'scan'})
             .search({'time': {'$gt': 0}}))
    assert bundle.uid in result


def test_repr(bundle):
    "Test that custom repr (with run uid) appears and is one line only."
    entry = bundle.cat['xyz']()[bundle.uid]
    assert bundle.uid in repr(entry)
    run = entry()
    assert bundle.uid in repr(run)
    assert len(repr(run).splitlines()) == 1


def test_repr_pretty(bundle):
    "Test the IPython _repr_pretty_ has uid and also stream names."
    formatters = pytest.importorskip("IPython.core.formatters")
    f = formatters.PlainTextFormatter()
    entry = bundle.cat['xyz']()[bundle.uid]
    assert bundle.uid in f(entry)
    # Stream names should be displayed.
    assert 'primary' in f(entry)
    run = entry()
    assert bundle.uid in f(run)
    assert 'primary' in f(run)


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


def test_canonical(bundle):
    run = bundle.cat['xyz']()[bundle.uid]

    filler = event_model.Filler({'NPY_SEQ': ophyd.sim.NumpySeqHandler},
                                inplace=False)

    # Smoke test for back-compat alias
    with pytest.warns(UserWarning):
        next(run.read_canonical())

    compare(run.canonical(fill='yes'),
            (filler(name, doc) for name, doc in bundle.docs))


def test_canonical_unfilled(bundle):
    run = bundle.cat['xyz']()[bundle.uid]
    run.canonical(fill='no')

    compare(run.canonical(fill='no'), bundle.docs)

    # Passing the run through the filler to check resource and datum are
    # received before corresponding event.
    filler = event_model.Filler({'NPY_SEQ': ophyd.sim.NumpySeqHandler},
                                inplace=False)
    for name, doc in run.canonical(fill='no'):
        filler(name, doc)


def test_canonical_delayed(bundle):
    run = bundle.cat['xyz']()[bundle.uid]

    filler = event_model.Filler({'NPY_SEQ': ophyd.sim.NumpySeqHandler},
                                inplace=False)

    if bundle.remote:
        with pytest.raises(NotImplementedError):
            next(run.canonical(fill='delayed'))
    else:
        compare(run.canonical(fill='delayed'),
                (filler(name, doc) for name, doc in bundle.docs))


def test_canonical_duplicates(bundle):
    run = bundle.cat['xyz']()[bundle.uid]
    history = set()
    run_start_uid = None

    for name, doc in run.canonical(fill='no'):
        if name == 'start':
            run_start_uid = doc['uid']
        elif name == 'datum':
            assert doc['datum_id'] not in history
            history .add(doc['datum_id'])
        elif name == 'datum_page':
            assert tuple(doc['datum_id']) not in history
            history.add(tuple(doc['datum_id']))
        elif name == 'event_page':
            for uid in doc['uid']:
                assert uid not in history
                history .add(uid)
        elif name == 'resource':
            assert doc.get('run_start', run_start_uid) == run_start_uid
            assert doc['uid'] not in history
            history.add(doc['uid'])
        else:
            assert doc['uid'] not in history
            history.add(doc['uid'])


def test_read(bundle):
    run = bundle.cat['xyz']()[bundle.uid]()
    entry = run['primary']
    entry.read()
    entry().to_dask()
    entry().to_dask().load()


def test_dot_access(bundle):
    run = bundle.cat['xyz']()[bundle.uid]()
    entry = run['primary']
    entry = getattr(run, 'primary')


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


def test_transforms(bundle):
    run = bundle.cat['xyz_with_transforms']()[bundle.uid]
    for name, doc in run.canonical(fill='no'):
        if name in {'start', 'stop', 'resource', 'descriptor'}:
            assert doc.get('test_key') == 'test_value'


def test_metadata_keys(bundle):
    run = bundle.cat['xyz']()[bundle.uid]()

    run_metadata = run.metadata
    assert 'start' in run_metadata
    assert 'stop' in run_metadata

    stream_metadata = run['primary']().metadata
    assert 'descriptors' in stream_metadata


def test_infinite_recursion_bug(bundle):
    run = bundle.cat['xyz']()[bundle.uid]()
    with pytest.raises(AttributeError):
        # used to raise RecursionErrror
        run.does_not_exist


def test_items(bundle):
    if bundle.remote:
        pytest.xfail("Regression in intake 0.6.0 awaiting patch")
    for uid, run in bundle.cat['xyz']().items():
        assert hasattr(run, 'canonical')

'''

def test_catalog_update(bundle, RE, hw):
    """
    Check that a new run is accessable with -1 immediatly after it is
    finished being serialized.
    """
    with bundle.serializer_partial() as serializer:
        new_uid = RE(count([hw.img]), serializer)[0]
        new_file = serializer.artifacts['all'][0]

    name, start_doc = next(bundle.cat['xyz']()[-1].canonical(fill='no'))
    assert start_doc['uid'] == new_uid
    os.unlink(new_file)
    bundle.cat['xyz'].force_reload()
    print(new_file)a

'''
