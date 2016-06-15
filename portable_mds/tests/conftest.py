import os
import tempfile
import shutil
import tzlocal
import pytest
from ..mongoquery.mds import MDS


@pytest.fixture(params=[1], scope='function')
def mds_all(request):
    '''Provide a function level scoped FileStore instance talking to
    temporary database on localhost:27017 with both v0 and v1.

    '''
    ver = request.param
    tempdirname = tempfile.mkdtemp()
    mds = MDS({'directory': tempdirname,
               'timezone': tzlocal.get_localzone().zone}, version=ver)
    filenames = ['run_starts.json', 'run_stops.json', 'event_descriptors.json',
                 'events.json']
    for fn in filenames:
        with open(os.path.join(tempdirname, fn), 'w') as f:
            f.write('[]')

    def delete_dm():
        shutil.rmtree(tempdirname)

    request.addfinalizer(delete_dm)

    return mds
