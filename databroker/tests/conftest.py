import os
import pytest
import sys
import uuid
from databroker.tests.utils import (build_sqlite_backed_broker,
                                    build_pymongo_backed_broker,
                                    build_hdf5_backed_broker,
                                    build_intake_jsonl_backed_broker,
                                    build_intake_mongo_backed_broker,
                                    build_intake_mongo_embedded_backed_broker)
import tempfile
import time
import requests.exceptions
import shutil
import tzlocal
import databroker.headersource.mongoquery as mqmds

from ..headersource import sqlite as sqlmds

if sys.version_info >= (3, 5):
    # this is a pytest.fixture
    from bluesky.tests.conftest import RE

    @pytest.fixture(scope='function')
    def hw(request):
        from ophyd.sim import hw
        return hw()

param_map = {'sqlite': build_sqlite_backed_broker,
             'mongo': build_pymongo_backed_broker,
             'hdf5': build_hdf5_backed_broker,
             'intake_jsonl': build_intake_jsonl_backed_broker,
             'intake_mongo': build_intake_mongo_backed_broker,
             # 'intake_mongo_embedded': build_intake_mongo_embedded_backed_broker,
             }

@pytest.fixture(params=list(param_map), scope='module')
def db(request):
    return param_map[request.param](request)


@pytest.fixture(params=list(param_map), scope='function')
def db_empty(request):
    if ('array_data' in request.function.__name__ and
            request.param == 'sqlite'):
        pytest.xfail('can not put lists into sqlite columns')
    return param_map[request.param](request)


@pytest.fixture(params=['sqlite', 'mongo', 'hdf5'], scope='function')
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
        pytest.skip("mds tests do not apply to intake-backed Broker")


@pytest.fixture(params=[mqmds,
                        sqlmds], scope='function')
def mds_portable(request):
    '''Provide a function level scoped Registry instance talking to
    temporary database on localhost:27017 with both v0 and v1.

    '''
    tempdirname = tempfile.mkdtemp()
    mds = request.param.MDS({'directory': tempdirname,
                             'timezone': tzlocal.get_localzone().zone,
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
