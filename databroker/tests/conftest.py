import shutil
import os
import tempfile
import tzlocal
from portable_mds.sqlite.mds import MDS
from portable_fs.sqlite.fs import FileStore
from databroker import Broker
from bluesky.tests.conftest import fresh_RE as RE
import pytest

@pytest.fixture(scope='function')
def db(request):
    tempdirname = tempfile.mkdtemp()
    mds = MDS({'directory': tempdirname,
                             'timezone': tzlocal.get_localzone().zone}, version=1)
    filenames = ['run_starts.json', 'run_stops.json', 'event_descriptors.json',
                 'events.json']
    for fn in filenames:
        with open(os.path.join(tempdirname, fn), 'w') as f:
            f.write('[]')

    def delete_mds():
        shutil.rmtree(tempdirname)

    request.addfinalizer(delete_mds)

    tf = tempfile.NamedTemporaryFile()
    fs = FileStore({'dbpath': tf.name}, version=1)

    def delete_fs():
        os.remove(tf.name)

    request.addfinalizer(delete_fs)

    db = Broker(mds, fs)
    return db
