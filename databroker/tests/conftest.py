import uuid
import shutil
import os
import sys
import tempfile
import tzlocal
from databroker import Broker
import pytest


if sys.version_info >= (3, 0):
    from bluesky.tests.conftest import fresh_RE as RE

@pytest.fixture(params=['sqlite', 'mongo'], scope='function')
def db(request):
    param_map = {'sqlite': build_sqlite_backed_broker,
                 'mongo': build_pymongo_backed_broker}

    return param_map[request.param](request)


def build_sqlite_backed_broker(request):
    """Uses mongoquery + sqlite -- no pymongo or mongo server anywhere"""
    from portable_mds.sqlite.mds import MDS
    from portable_fs.sqlite.fs import FileStore

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

    return Broker(mds, fs)


def build_pymongo_backed_broker(request):
    '''Provide a function level scoped MDS instance talking to
    temporary database on localhost:27017 with v1 schema.

    '''
    from metadatastore.mds import MDS
    from filestore.utils import create_test_database
    from filestore.fs import FileStore

    db_name = "mds_testing_disposable_{}".format(str(uuid.uuid4()))
    test_conf = dict(database=db_name, host='localhost',
                     port=27017, timezone='US/Eastern')
    mds = MDS(test_conf, 1, auth=False)

    def delete_mds():
        print("DROPPING DB")
        mds._connection.drop_database(db_name)

    request.addfinalizer(delete_mds)

    db_name = "fs_testing_base_disposable_{uid}"
    test_conf = create_test_database(host='localhost',
                                     port=27017, version=1,
                                     db_template=db_name)
    fs = FileStore(test_conf, version=1)

    def delete_fs():
        print("DROPPING DB")
        fs._connection.drop_database(test_conf['database'])

    request.addfinalizer(delete_fs)

    return Broker(mds, fs)
