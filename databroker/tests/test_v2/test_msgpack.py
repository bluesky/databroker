import intake
from functools import partial
from suitcase.msgpack import Serializer
import os
from pathlib import Path
import pytest
import shutil
import tempfile
import time
import types

from .generic import *  # noqa

TMP_DIRS = {param: tempfile.mkdtemp() for param in ['local', 'remote']}
TEST_CATALOG_PATH = TMP_DIRS['remote']  # used by intake_server fixture

YAML_FILENAME = 'intake_test_catalog.yml'


def teardown_module(module):
    for path in TMP_DIRS.values():
        try:
            shutil.rmtree(path)
        except BaseException:
            pass


@pytest.fixture(params=['local'], scope='module')
def bundle(request, intake_server, example_data):  # noqa
    tmp_dir = TMP_DIRS[request.param]
    tmp_data_dir = Path(tmp_dir) / 'data'
    serializer_partial = partial(Serializer, tmp_data_dir)
    serializer = serializer_partial()
    uid, docs = example_data
    for name, doc in docs:
        serializer(name, doc)
    serializer.close()
    fullname = os.path.join(tmp_dir, YAML_FILENAME)
    with open(fullname, 'w') as f:
        f.write(f'''
sources:
  xyz:
    description: Some imaginary beamline
    driver: "bluesky-msgpack-catalog"
    container: catalog
    args:
      paths: {tmp_data_dir / "*.msgpack"}
      handler_registry:
        NPY_SEQ: ophyd.sim.NumpySeqHandler
    metadata:
      beamline: "00-ID"
  xyz_with_transforms:
    description: Some imaginary beamline
    driver: "bluesky-msgpack-catalog"
    container: catalog
    args:
      paths: {tmp_data_dir / "*.msgpack"}
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
        cat = intake.open_catalog(os.path.join(tmp_dir, YAML_FILENAME))
    elif request.param == 'remote':
        cat = intake.open_catalog(intake_server, page_size=10)
    else:
        raise ValueError
    return types.SimpleNamespace(cat=cat,
                                 uid=uid,
                                 docs=docs,
                                 remote=remote,
                                 serializer_partial=serializer_partial)
