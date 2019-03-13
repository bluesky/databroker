import intake_bluesky.jsonl # noqa
import intake
from suitcase.jsonl import Serializer
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
def bundle(request, intake_server, example_data, tmp_path):  # noqa
    serializer = Serializer(tmp_path)
    uid, docs = example_data
    for name, doc in docs:
        serializer(name, doc)
    serializer.close()

    fullname = os.path.join(TMP_DIR, YAML_FILENAME)
    with open(fullname, 'w') as f:
        f.write(f'''
plugins:
  source:
    - module: intake_bluesky
sources:
  xyz:
    description: Some imaginary beamline
    driver: intake_bluesky.jsonl.BlueskyJSONLCatalog
    container: catalog
    args:
      paths: {[str(path) for path in serializer.artifacts['all']]}
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
