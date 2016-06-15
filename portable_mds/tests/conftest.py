import os
import tempfile
import shutil
import tzlocal
import pytest
import portable_mds.mongoquery.mds
import portable_mds.sqlite.mds
import portable_mds.hdf5.mds

variations = [portable_mds.mongoquery.mds,
              portable_mds.sqlite.mds,
              portable_mds.hdf5.mds]

@pytest.fixture(params=variations, scope='function')
def mds_all(request):
    '''Provide a function level scoped FileStore instance talking to
    temporary database on localhost:27017 with both v0 and v1.

    '''
    tempdirname = tempfile.mkdtemp()
    mds = request.param.MDS({'directory': tempdirname,
               'timezone': tzlocal.get_localzone().zone}, version=1)
    filenames = ['run_starts.json', 'run_stops.json', 'event_descriptors.json',
                 'events.json']
    for fn in filenames:
        with open(os.path.join(tempdirname, fn), 'w') as f:
            f.write('[]')

    def delete_dm():
        shutil.rmtree(tempdirname)

    request.addfinalizer(delete_dm)

    return mds
