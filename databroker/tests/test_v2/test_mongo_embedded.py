import intake
from functools import partial
from suitcase.mongo_embedded import Serializer
import os
import pytest
import shutil
import tempfile
import time
import types

from .generic import *  # noqa

TMP_DIR = tempfile.mkdtemp()
TEST_CATALOG_PATH = [TMP_DIR]

YAML_FILENAME = 'intake_test_catalog.yml'


def teardown_module(module):
    try:
        shutil.rmtree(TMP_DIR)
    except BaseException:
        pass


@pytest.fixture(params=['local'], scope='module')
def bundle(request, intake_server, example_data, db_factory):  # noqa
    fullname = os.path.join(TMP_DIR, YAML_FILENAME)
    permanent_db = db_factory()
    serializer_partial = partial(Serializer, permanent_db)
    serializer = serializer_partial()
    uid, docs = example_data
    for name, doc in docs:
        serializer(name, doc)

    def extract_uri(db):
        return f'mongodb://{db.client.address[0]}:{db.client.address[1]}/{db.name}'

    with open(fullname, 'w') as f:
        f.write(f'''
sources:
  xyz:
    description: Some imaginary beamline
    driver: "bluesky-mongo-embedded-catalog"
    container: catalog
    args:
      datastore_db: {extract_uri(permanent_db)}
      handler_registry:
        NPY_SEQ: ophyd.sim.NumpySeqHandler
    metadata:
      beamline: "00-ID"
  xyz_with_transforms:
    description: Some imaginary beamline
    driver: "bluesky-mongo-embedded-catalog"
    container: catalog
    args:
      datastore_db: {extract_uri(permanent_db)}
      handler_registry:
        NPY_SEQ: ophyd.sim.NumpySeqHandler
      transforms:
        start: databroker.tests.test_v2.transform.transform
        stop: databroker.tests.test_v2.transform.transform
        resource: databroker.tests.test_v2.transform.transform
        descriptor: databroker.tests.test_v2.transform.transform
    metadata:
      beamline: "00-ID"
        ''')

    time.sleep(2)
    remote = request.param == 'remote'

    if request.param == 'local':
        cat = intake.open_catalog(os.path.join(TMP_DIR, YAML_FILENAME))
    elif request.param == 'remote':
        cat = intake.open_catalog(intake_server, page_size=10)
    else:
        raise ValueError
    return types.SimpleNamespace(cat=cat,
                                 uid=uid,
                                 docs=docs,
                                 remote=remote,
                                 serializer_partial=serializer_partial)
