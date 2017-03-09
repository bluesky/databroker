import os
import tempfile
import shutil
import tzlocal
import pytest
from .. import mds

variations = [mds]


@pytest.fixture(params=variations, scope='function')
def mds_all(request):
    '''Provide a function level scoped FileStore instance talking to
    temporary database on localhost:27017 with both v0 and v1.
    '''
    try:
        tmds = request.param.MDS({'host': 'localhost',
                                 'port': 7778,
                                 'timezone': 'US/Eastern'})
    except AttributeError:
        request.param._DB_SINGLETON = metadataclient.mds.MDS({'host': 'localhost',
                                                              'port': 7778,
                                                              'timezone': 'US/Eastern'})
        tmds = request.param._DB_SINGLETON
    return tmds
