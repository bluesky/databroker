import os
import pytest
import sys
import uuid
import ujson
from databroker.tests.utils import (build_sqlite_backed_broker,
                                    build_pymongo_backed_broker,
                                    build_hdf5_backed_broker,
                                    build_client_backend_broker,
                                    start_md_server,
                                    stop_md_server)
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


@pytest.fixture(params=['sqlite', 'mongo', 'hdf5', 'client'], scope='module')
def db(request):
    param_map = {'sqlite': build_sqlite_backed_broker,
                 'mongo': build_pymongo_backed_broker,
                 'hdf5': build_hdf5_backed_broker,
                 'client': build_client_backend_broker}

    return param_map[request.param](request)


@pytest.fixture(params=['sqlite', 'mongo', 'hdf5', 'client'], scope='function')
def db_empty(request):
    param_map = {'sqlite': build_sqlite_backed_broker,
                 'mongo': build_pymongo_backed_broker,
                 'hdf5': build_hdf5_backed_broker,
                 'client': build_client_backend_broker}
    if ('array_data' in request.function.__name__ and
            request.param == 'sqlite'):
        pytest.xfail('can not put lists into sqlite columns')

    return param_map[request.param](request)


@pytest.fixture(params=['sqlite', 'mongo', 'hdf5', 'client'], scope='function')
def broker_factory(request):
    "Use this to get more than one broker in a test."
    param_map = {'sqlite': lambda: build_sqlite_backed_broker(request),
                 'mongo': lambda: build_pymongo_backed_broker(request),
                 'hdf5': lambda: build_hdf5_backed_broker(request),
                 'client': lambda: build_client_backend_broker(request)}

    return param_map[request.param]


AUTH = os.environ.get('MDSTESTWITHAUTH', False)


@pytest.fixture(scope='module')
def mds_all(request, db):
    '''Provide a function level scoped Registry instance talking to
    temporary database on localhost:27017 with both v0 and v1.
    '''
    return db.mds


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


@pytest.fixture(scope='module')
def md_server_url(request):
    from random import randint
    port = randint(9000, 60000)
    testing_config = dict(mongohost='localhost', mongoport=27017,
                          database='mds_test'+str(uuid.uuid4()),
                          serviceport=port, tzone='US/Eastern')

    proc = start_md_server(testing_config)

    def tear_down():
        stop_md_server(proc, testing_config)

    request.addfinalizer(tear_down)
    base_url = 'http://{}:{}/'.format('localhost',
                                      testing_config['serviceport'])

    # Wait here until the server responds. Time out after 1 minute.
    TIMEOUT = 60  # seconds
    startup_time = time.time()
    url = base_url + 'run_start'
    message = dict(query={}, signature='find_run_starts')
    print("Waiting up to 60 seconds for the server to start up....")
    while True:
        if time.time() - startup_time > TIMEOUT:
            raise Exception("Server startup timed out.")
        try:
            requests.get(url, params=ujson.dumps(message))
        except requests.exceptions.ConnectionError:
            time.sleep(1)
            continue
        else:
            break
    print("Server is up!")

    return base_url
