import copy
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
from .. import mongo_normalized
import suitcase.mongo_normalized
import suitcase.mongo_embedded
from tiled.client import Context, from_context
from tiled.server.app import build_app
from tiled.media_type_registration import default_serialization_registry
from bluesky_tiled_plugins.exporters import json_seq_exporter
from tiled.catalog import from_uri


def get_uids(result):
    if hasattr(result, "run_start_uids"):
        return result.run_start_uids
    else:
        return result


def build_tiled_sqlite_backed_broker(request):
    serialization_registry = copy.copy(default_serialization_registry)
    serialization_registry.register(
        "BlueskyRun", "application/json-seq", json_seq_exporter
    )
    tmpdir = tempfile.TemporaryDirectory()
    adapter = from_uri(
        f"sqlite:///{tmpdir.name}/catalog.db",
        specs=[{"name": "CatalogOfBlueskyRuns", "version": "3.0"}],
        init_if_not_exists=True,
        writable_storage=[
            f"{tmpdir.name}/data_files",
            f"sqlite:///{tmpdir.name}/tabular_data.db",
        ],
        # The ophyd.sim img device generates its own directory somewhere
        # in /tmp (or Windows equivalent) and we need that to be readable.
        readable_storage=[
            tempfile.gettempdir(),
        ]
    )
    context = Context.from_app(build_app(adapter, serialization_registry=serialization_registry))
    client = from_context(context)

    def teardown():
        context.__exit__()
        tmpdir.cleanup()

    request.addfinalizer(teardown)

    return client.v1


def build_tiled_mongo_backed_broker(request):
    adapter = mongo_normalized.MongoAdapter.from_mongomock(
        handler_registry={'NPY_SEQ': ophyd.sim.NumpySeqHandler})
    context = Context.from_app(build_app(adapter))
    client = from_context(context)

    def teardown():
        context.__exit__()

    request.addfinalizer(teardown)

    return client.v1


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

    # Create indexes.
    serializer = suitcase.mongo_normalized.Serializer(mds._db, fs._db)
    serializer.create_indexes()
    return v0.Broker(mds, fs)
