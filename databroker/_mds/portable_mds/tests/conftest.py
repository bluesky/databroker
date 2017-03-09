import os
import tempfile
import shutil
import tzlocal
import pytest
from ..mongoquery import mds as mqmds
from ..sqlite import mds as sqlmds
from ..hdf5 import mds as hdfmds

variations = [mqmds,
              sqlmds,
              hdfmds]


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
