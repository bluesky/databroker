import os
import shutil
import tempfile
import uuid

import tzlocal

from databroker import Broker
from databroker.broker import HeaderSourceShim, BrokerES
from databroker.eventsource import EventSourceShim


def build_sqlite_backed_broker(request):
    """Uses mongoquery + sqlite -- no pymongo or mongo server anywhere"""
    from ..headersource.sqlite import MDS
    from ..resource_registry.sqlite import FileStore

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

    return BrokerES(HeaderSourceShim(mds),
                    EventSourceShim(mds, fs))


def build_hdf5_backed_broker(request):
    from ..headersource.hdf5 import MDS
    from ..resource_registry.sqlite import FileStore

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

    return BrokerES(HeaderSourceShim(mds),
                    EventSourceShim(mds, fs))


def build_pymongo_backed_broker(request):
    '''Provide a function level scoped MDS instance talking to
    temporary database on localhost:27017 with v1 schema.

    '''
    from ..headersource.mongo import MDS
    from ..resource_registry.utils import create_test_database
    from ..resource_registry.mongo import FileStore

    db_name = "mds_testing_disposable_{}".format(str(uuid.uuid4()))
    md_test_conf = dict(database=db_name, host='localhost',
                        port=27017, timezone='US/Eastern')
    mds = MDS(md_test_conf, 1, auth=False)

    db_name = "fs_testing_base_disposable_{uid}"
    fs_test_conf = create_test_database(host='localhost',
                                        port=27017, version=1,
                                        db_template=db_name)
    fs = FileStore(fs_test_conf, version=1)

    def delete_fs():
        print("DROPPING DB")
        fs._connection.drop_database(fs_test_conf['database'])
        mds._connection.drop_database(md_test_conf['database'])

    request.addfinalizer(delete_fs)

    return Broker(mds, fs)
