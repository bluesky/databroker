import asyncio
from distutils.version import LooseVersion
import uuid

import bluesky
from bluesky.run_engine import RunEngine, TransitionError
from bluesky.plans import scan
from bluesky.preprocessors import SupplementalData
import event_model
import ophyd.sim
import pymongo
import pytest


# Make module-scoped versions of these fixtures to avoid paying for
# intake-server startup each time.


@pytest.fixture(scope='module')
def hw():
    return ophyd.sim.hw()  # a SimpleNamespace of simulated devices


@pytest.fixture(scope='module')
def RE(request):
    loop = asyncio.new_event_loop()
    loop.set_debug(True)
    RE = RunEngine({}, loop=loop)

    def clean_event_loop():
        if RE.state not in ('idle', 'panicked'):
            try:
                RE.halt()
            except TransitionError:
                pass
        loop.call_soon_threadsafe(loop.stop)
        if LooseVersion(bluesky.__version__) >= LooseVersion('1.6.0'):
            RE._th.join()
        loop.close()

    request.addfinalizer(clean_event_loop)
    return RE


SIM_DETECTORS = {'scalar': 'det',
                 'image': 'direct_img',
                 'external_image': 'img'}


@pytest.fixture(params=['scalar', 'image', 'external_image'], scope='module')
def detector(request, hw):
    return getattr(hw, SIM_DETECTORS[request.param])


@pytest.fixture(scope='module')
def example_data(hw, detector, RE):  # noqa
    sd = SupplementalData(baseline=[hw.motor])
    RE.preprocessors.append(sd)

    docs = []

    def collect(name, doc):
        docs.append((name, event_model.sanitize_doc(doc)))

    uid, = RE(scan([detector], hw.motor, -1, 1, 20), collect)
    return uid, docs


@pytest.fixture(scope='module')
def db_factory(request):
    def inner():
        database_name = f'test-{str(uuid.uuid4())}'
        uri = f'mongodb://localhost:27017/'
        client = pymongo.MongoClient(uri)

        def drop():
            client.drop_database(database_name)

        request.addfinalizer(drop)
        return client[database_name]
    return inner
