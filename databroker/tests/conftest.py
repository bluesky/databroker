import os
import pytest
import sys
import uuid
from databroker.headersource.mongo import MDS
from databroker.tests.utils import (build_sqlite_backed_broker,
                                    build_pymongo_backed_broker,
                                    build_hdf5_backed_broker)

if sys.version_info >= (3, 0):
    from bluesky.tests.conftest import fresh_RE as RE


@pytest.fixture(params=['sqlite', 'mongo', 'hdf5'], scope='function')
def db(request):
    param_map = {'sqlite': build_sqlite_backed_broker,
                 'mongo': build_pymongo_backed_broker,
                 'hdf5': build_hdf5_backed_broker}

    return param_map[request.param](request)


@pytest.fixture(params=['sqlite', 'mongo', 'hdf5'], scope='function')
def broker_factory(request):
    "Use this to get more than one broker in a test."
    param_map = {'sqlite': lambda: build_sqlite_backed_broker(request),
                 'mongo': lambda: build_pymongo_backed_broker(request),
                 'hdf5': lambda: build_hdf5_backed_broker(request)}

    return param_map[request.param]


AUTH = os.environ.get('MDSTESTWITHAUTH', False)


@pytest.fixture(params=['sqlite', 'mongo', 'hdf5'], scope='function')
def mds_all(request):
    '''Provide a function level scoped FileStore instance talking to
    temporary database on localhost:27017 with both v0 and v1.
    '''
    param_map = {'sqlite': build_sqlite_backed_broker,
                 'mongo': build_pymongo_backed_broker,
                 'hdf5': build_hdf5_backed_broker}

    return param_map[request.param](request).mds
