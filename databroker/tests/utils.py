import os
from pathlib import Path
from subprocess import Popen
import shutil
import tempfile
import time
import uuid
import pytest
import ophyd.sim
import tzlocal

from databroker import v0, v1
from databroker.headersource import HeaderSourceShim
from databroker.eventsource import EventSourceShim
from ..from_files import JSONLTree, MsgpackTree
from .. import mongo_normalized
import suitcase.jsonl
import suitcase.mongo_normalized
import suitcase.mongo_embedded
from tiled.client import from_tree

def get_uids(result):
    if hasattr(result, "run_start_uids"):
        return result.run_start_uids
    else:
        return result


def build_jsonl_backed_broker(request):
    tmp_dir = tempfile.TemporaryDirectory()

    def teardown():
        tmp_dir.cleanup()

    request.addfinalizer(teardown)
    broker = JSONLTree.from_directory(
        tmp_dir.name,
        handler_registry={'NPY_SEQ': ophyd.sim.NumpySeqHandler})
    return from_tree(broker).v1


def build_tiled_mongo_backed_broker(request):
    adapter = mongo_normalized.MongoAdapter.from_mongomock(
        handler_registry={'NPY_SEQ': ophyd.sim.NumpySeqHandler})
    return from_tree(adapter).v1


def build_sqlite_backed_broker(request):
    """Uses mongoquery + sqlite -- no pymongo or mongo server anywhere"""

    config = v0.temp_config()
    tempdir = config['metadatastore']['config']['directory']

    def cleanup():
        shutil.rmtree(tempdir)

    request.addfinalizer(cleanup)

    return v0.Broker.from_config(config)


def build_hdf5_backed_broker(request):
    from ..headersource.hdf5 import MDS
    from ..assets.sqlite import Registry

    tz = tzlocal.get_localzone()
    try:
        tz = tz.key
    except AttributeError:
        tz = tz.zone

    tempdirname = tempfile.mkdtemp()
    mds = MDS({'directory': tempdirname,
               'timezone': tz,
               'version': 1})
    filenames = ['run_starts.json', 'run_stops.json', 'event_descriptors.json',
                 'events.json']
    for fn in filenames:
        with open(os.path.join(tempdirname, fn), 'w') as f:
            f.write('[]')

    def delete_mds():
        shutil.rmtree(tempdirname)

    request.addfinalizer(delete_mds)

    tf = tempfile.NamedTemporaryFile()
    fs = Registry({'dbpath': tf.name})

    def delete_fs():
        os.remove(tf.name)

    request.addfinalizer(delete_fs)

    return v0.BrokerES(HeaderSourceShim(mds),
                       [EventSourceShim(mds, fs)],
                       {'': fs}, {}, name=None)


def build_legacy_mongo_backed_broker(request):
    '''Provide a function level scoped MDS instance talking to
    temporary database on localhost:27017 with v1 schema.

    '''
    from ..headersource.mongo import MDS
    from ..assets.utils import create_test_database
    from ..assets.mongo import Registry

    db_name = "mds_testing_disposable_{}".format(str(uuid.uuid4()))
    md_test_conf = dict(database=db_name, host='localhost',
                        port=27017, timezone='US/Eastern',
                        version=1)
    mds = MDS(md_test_conf, auth=False)

    db_name = "fs_testing_base_disposable_{uid}"
    fs_test_conf = create_test_database(host='localhost',
                                        port=27017, version=1,
                                        db_template=db_name)
    fs = Registry(fs_test_conf)

    def delete_fs():
        print("DROPPING DB")
        fs._connection.drop_database(fs_test_conf['database'])
        mds._connection.drop_database(md_test_conf['database'])

    request.addfinalizer(delete_fs)

    return v0.Broker(mds, fs)
