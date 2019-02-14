import intake_bluesky.mongo_layout1  # noqa
from bluesky import RunEngine
from bluesky.plans import scan
from bluesky.preprocessors import SupplementalData
import event_model
import itertools
import intake
from intake.conftest import intake_server  # noqa
from suitcase.mongo_layout1.tests.conftest import db_factory  # noqa
import json
from suitcase.mongo_layout1 import Serializer
import numpy
from ophyd.sim import motor, det, img, direct_img, NumpySeqHandler
import os
import pytest
import shutil
import tempfile
import time
import types


TMP_DIR = tempfile.mkdtemp()
TEST_CATALOG_PATH = [TMP_DIR]

YAML_FILENAME = 'intake_test_catalog.yml'


def teardown_module(module):
    try:
        shutil.rmtree(TMP_DIR)
    except BaseException:
        pass


def normalize(doc):
    # numpy arrays -> lists (via sanitize doc)
    # tuples -> lists (via json dump/load)
    return json.loads(json.dumps(event_model.sanitize_doc(doc)))


@pytest.fixture(params=['local', 'remote'])
def bundle(request, intake_server, db_factory):  # noqa
    "A SimpleNamespace with an intake_server and some uids of sample data."
    fullname = os.path.join(TMP_DIR, YAML_FILENAME)

    RE = RunEngine({})
    sd = SupplementalData(baseline=[motor])
    RE.preprocessors.append(sd)
    mds_db = db_factory()
    assets_db = db_factory()
    serializer = Serializer(mds_db, assets_db)
    RE.subscribe(serializer)

    # Simulate data with a scalar detector.
    det_scan_docs = []

    def collect(name, doc):
        doc = normalize(doc)
        det_scan_docs.append((name, doc))

    det_scan_uid, = RE(scan([det], motor, -1, 1, 20), collect)

    # Simulate data with an array detector.
    direct_img_scan_docs = []

    def collect(name, doc):
        doc = normalize(doc)
        direct_img_scan_docs.append((name, doc))

    direct_img_scan_uid, = RE(scan([direct_img], motor, -1, 1, 20), collect)

    # Simulate data with an array detector that stores its data externally.
    img_scan_docs = []

    def collect(name, doc):
        doc = normalize(doc)
        img_scan_docs.append((name, doc))

    img_scan_uid, = RE(scan([img], motor, -1, 1, 20), collect)

    def extract_uri(db):
        return f'mongodb://{db.client.address[0]}:{db.client.address[1]}/{db.name}'

    with open(fullname, 'w') as f:
        f.write(f'''
plugins:
  source:
    - module: intake_bluesky
sources:
  xyz:
    description: Some imaginary beamline
    driver: intake_bluesky.mongo_layout1.BlueskyMongoCatalog
    container: catalog
    args:
      metadatastore_db: {extract_uri(mds_db)}
      asset_registry_db: {extract_uri(assets_db)}
      handler_registry:
        NPY_SEQ: ophyd.sim.NumpySeqHandler
    metadata:
      beamline: "00-ID"
        ''')

    time.sleep(2)

    if request.param == 'local':
        cat = intake.Catalog(os.path.join(TMP_DIR, YAML_FILENAME))
    elif request.param == 'remote':
        cat = intake.Catalog(intake_server, page_size=10)
    else:
        raise ValueError
    yield types.SimpleNamespace(intake_server=intake_server,
                                cat=cat,
                                det_scan_uid=det_scan_uid,
                                det_scan_docs=det_scan_docs,
                                direct_img_scan_uid=direct_img_scan_uid,
                                direct_img_scan_docs=direct_img_scan_docs,
                                img_scan_uid=img_scan_uid,
                                img_scan_docs=img_scan_docs)


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
             .search({'detectors': 'det'}))
    assert name == bundle.det_scan_uid


def test_run_metadata(bundle):
    "Find 'start' and 'stop' in the Entry metadata."
    run = bundle.cat['xyz']()[bundle.det_scan_uid]
    for key in ('start', 'stop'):
        assert key in run.metadata  # entry
        assert key in run().metadata  # datasource


def test_read_canonical_scalar(bundle):
    run = bundle.cat['xyz']()[bundle.det_scan_uid]
    run.read_canonical()

    def sorted_actual():
        for name_ in ('start', 'descriptor', 'event', 'stop'):
            for name, doc in bundle.det_scan_docs:
                if name == name_:
                    yield name, doc

    for actual, expected in zip(run.read_canonical(), sorted_actual()):
        actual_name, actual_doc = actual
        expected_name, expected_doc = expected
        assert actual_name == expected_name
        assert actual_doc == expected_doc


def test_read_canonical_external(bundle):
    run = bundle.cat['xyz']()[bundle.img_scan_uid]
    run.read_canonical()
    filler = event_model.Filler({'NPY_SEQ': NumpySeqHandler})

    def sorted_actual():
        for name_ in ('start', 'descriptor', 'resource', 'datum', 'event_page', 'event', 'stop'):
            for name, doc in bundle.img_scan_docs:
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


def test_read_canonical_nonscalar(bundle):
    run = bundle.cat['xyz']()[bundle.direct_img_scan_uid]
    run.read_canonical()

    def sorted_actual():
        for name_ in ('start', 'descriptor', 'event', 'stop'):
            for name, doc in bundle.direct_img_scan_docs:
                if name == name_:
                    yield name, doc

    for actual, expected in zip(run.read_canonical(), sorted_actual()):
        actual_name, actual_doc = actual
        expected_name, expected_doc = expected
        assert actual_name == expected_name
        assert actual_doc == expected_doc


def test_access_scalar_data(bundle):
    "Access simple scalar data that is stored directly in Event documents."
    run = bundle.cat['xyz']()[bundle.det_scan_uid]()
    entry = run['primary']
    entry.read()
    entry().to_dask()
    entry().to_dask().load()


def test_include_and_exclude(bundle):
    "Access simple scalar data that is stored directly in Event documents."
    run = bundle.cat['xyz']()[bundle.det_scan_uid]()
    entry = run['primary']
    assert 'motor' in entry().read().variables
    assert 'motor' not in entry(exclude=['motor']).read().variables
    assert 'det' in entry(exclude=['motor']).read().variables
    expected = set(['time', 'uid', 'seq_num', 'det'])
    assert set(entry(include=['det']).read().variables) == expected
    expected = set(['time', 'uid', 'seq_num', 'motor:motor_velocity'])
    assert set(entry(include=['motor:motor_velocity']).read().variables) == expected


def test_access_nonscalar_data(bundle):
    "Access nonscalar data that is stored directly in Event documents."
    run = bundle.cat['xyz']()[bundle.direct_img_scan_uid]()
    entry = run['primary']
    entry.read()
    entry().to_dask()
    entry().to_dask().load()


def test_access_external_data(bundle):
    "Access nonscalar data that is stored directly in Event documents."
    run = bundle.cat['xyz']()[bundle.img_scan_uid]()
    entry = run['primary']
    entry.read()
    entry().to_dask()
    entry().to_dask().load()
