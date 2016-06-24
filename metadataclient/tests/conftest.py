import os
import tempfile
import shutil
import tzlocal
import pytest
# from metadataclient import mds
import metadataclient.mds
# import portable_mds.mongoquery.mds
# import portable_mds.sqlite.mds
# import portable_mds.hdf5.mds

variations = [metadataclient.mds]

@pytest.fixture(params=variations, scope='function')
def mds_all(request):
    '''Provide a function level scoped FileStore instance talking to
    temporary database on localhost:27017 with both v0 and v1.
    '''
    mds = request.param.MDS({'host': 'localhost',
               'port': 7778})
    return mds
