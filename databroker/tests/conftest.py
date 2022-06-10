import os
import pytest
import sys
from databroker.tests.utils import (build_sqlite_backed_broker,
                                    build_legacy_mongo_backed_broker,
                                    build_jsonl_backed_broker,
                                    build_tiled_mongo_backed_broker,
                                    )
import tempfile
import shutil
import tzlocal
import databroker.headersource.mongoquery as mqmds

from ..headersource import sqlite as sqlmds

if sys.version_info >= (3, 5):
    # this is a pytest.fixture
    from bluesky.tests.conftest import RE  # noqa: F401

    @pytest.fixture(scope='function')
    def hw(request):
        from ophyd.sim import hw
        return hw()

param_map = {'sqlite-legacy': build_sqlite_backed_broker,
             'mongo-legacy': build_legacy_mongo_backed_broker,
             'jsonl': build_jsonl_backed_broker,
             'mongo-tiled': build_tiled_mongo_backed_broker,
             }
params = [
    # Apply the mark pytest.mark.flaky to a *fixture* as shown in
    # https://github.com/pytest-dev/pytest/issues/3969#issuecomment-420511822
    # pytest.param('sqlite', marks=pytest.mark.flaky(reruns=1, reruns_delay=2)),
    'mongo-legacy',
    'mongo-tiled',
    'sqlite-legacy',
    # 'jsonl',
]


@pytest.fixture(params=params, scope='module')
def db(request):
    return param_map[request.param](request)


@pytest.fixture
def c(request):
    return build_tiled_mongo_backed_broker(request).v2


@pytest.fixture(params=params, scope='function')
def db_empty(request):
    if ('array_data' in request.function.__name__ and
            request.param == 'sqlite-legacy'):
        pytest.xfail('can not put lists into sqlite columns')
    return param_map[request.param](request)


@pytest.fixture(params=params, scope='function')
def broker_factory(request):
    "Use this to get more than one broker in a test."
    return {k: lambda: v(request) for k, v in param_map.items()}[request.param]


AUTH = os.environ.get('MDSTESTWITHAUTH', False)


@pytest.fixture(scope='module')
def mds_all(request, db):
    '''Provide a function level scoped Registry instance talking to
    temporary database on localhost:27017 with both v0 and v1.
    '''
    try:
        return db.mds
    except AttributeError:
        pytest.skip("mds tests do not apply to tiled-backed Broker")


@pytest.fixture(params=[mqmds,
                        sqlmds], scope='function')
def mds_portable(request):
    '''Provide a function level scoped Registry instance talking to
    temporary database on localhost:27017 with both v0 and v1.

    '''
    tz = tzlocal.get_localzone()
    try:
        tz = tz.key
    except AttributeError:
        tz = tz.zone
    tempdirname = tempfile.mkdtemp()
    mds = request.param.MDS({'directory': tempdirname,
                             'timezone': tz,
                             'version': 1})
    filenames = ['run_starts.json', 'run_stops.json', 'event_descriptors.json',
                 'events.json']
    for fn in filenames:
        with open(os.path.join(tempdirname, fn), 'w') as f:
            f.write('[]')

    def delete_dm():
        shutil.rmtree(tempdirname)

    request.addfinalizer(delete_dm)

    return mds


SIM_DETECTORS = {'scalar': 'det',
                 'image': 'direct_img',
                 'external_image': 'img'}


@pytest.fixture(params=['scalar', 'image', 'external_image'])
def detector(request, hw):
    return getattr(hw, SIM_DETECTORS[request.param])
