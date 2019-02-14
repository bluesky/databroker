import intake_bluesky.mongo_layout1  # noqa
from bluesky import RunEngine
from bluesky.plans import scan
from bluesky.preprocessors import SupplementalData
import event_model
import itertools
import intake
import json
from suitcase.mongo_layout1 import Serializer
import numpy
import ophyd.sim
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


@pytest.fixture
def hw():
    return ophyd.sim.hw()  # a SimpleNamespace of simulated devices


SIM_DETECTORS = {'scalar': 'det',
                 'image': 'direct_img',
                 'external_image': 'img'}


@pytest.fixture(params=['scalar', 'image', 'external_image'])
def detector(request, hw):
    return getattr(hw, SIM_DETECTORS[request.param])


@pytest.fixture
def example_data(hw, detector):
    RE = RunEngine({})
    sd = SupplementalData(baseline=[hw.motor])
    RE.preprocessors.append(sd)

    docs = []

    def collect(name, doc):
        doc = normalize(doc)
        docs.append((name, doc))

    uid, = RE(scan([detector], hw.motor, -1, 1, 20), collect)
    return uid, docs


@pytest.fixture(params=['local', 'remote'])
def bundle(request, intake_server, example_data, db_factory):  # noqa
    fullname = os.path.join(TMP_DIR, YAML_FILENAME)
    mds_db = db_factory()
    assets_db = db_factory()
    serializer = Serializer(mds_db, assets_db)
    uid, docs = example_data
    for name, doc in docs:
        serializer(name, doc)

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
    return types.SimpleNamespace(cat=cat,
                                 uid=uid,
                                 docs=docs)


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


def test_access_scalar_data(bundle):
    "Access simple scalar data that is stored directly in Event documents."
    run = bundle.cat['xyz']()[bundle.uid]()
    entry = run['primary']
    entry.read()
    entry().to_dask()
    entry().to_dask().load()


def test_include_and_exclude(bundle):
    "Access simple scalar data that is stored directly in Event documents."
    run = bundle.cat['xyz']()[bundle.uid]()
    entry = run['primary']
    assert 'motor' in entry().read().variables
    assert 'motor' not in entry(exclude=['motor']).read().variables
    assert 'motor' in entry(exclude=['NONEXISTENT']).read().variables
    expected = set(['time', 'uid', 'seq_num', 'motor'])
    assert set(entry(include=['motor']).read().variables) == expected
    expected = set(['time', 'uid', 'seq_num', 'motor:motor_velocity'])
    assert set(entry(include=['motor:motor_velocity']).read().variables) == expected
