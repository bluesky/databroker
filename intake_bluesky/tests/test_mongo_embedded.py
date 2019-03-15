import intake_bluesky.mongo_embedded  # noqa
import intake
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


@pytest.fixture(params=['local', 'remote'])
def bundle(request, intake_server, example_data, db_factory):  # noqa
    fullname = os.path.join(TMP_DIR, YAML_FILENAME)
    volatile_db = db_factory()
    permanent_db = db_factory()
    serializer = Serializer(volatile_db, permanent_db)
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
    driver: intake_bluesky.mongo_embedded.BlueskyMongoCatalog
    container: catalog
    args:
      datastore_db: {extract_uri(permanent_db)}
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
