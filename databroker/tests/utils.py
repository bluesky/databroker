import os
from pathlib import Path
import shutil
import tempfile
import uuid

import mongobox
import tzlocal

from databroker import v0, v1
from databroker.headersource import HeaderSourceShim
from databroker.eventsource import EventSourceShim
from .._drivers import jsonl
from .._drivers import mongo_normalized
from .._drivers import mongo_embedded
from .. import core
import suitcase.jsonl
import suitcase.mongo_normalized
import suitcase.mongo_embedded


def build_intake_jsonl_backed_broker(request):
    tmp_dir = tempfile.TemporaryDirectory()
    tmp_path = tmp_dir.name
    catalog_path = Path(tmp_path) / 'catalog.yml'
    data_dir = Path(tmp_path) / 'data'
    with open(catalog_path, 'w') as file:
        file.write(f"""
plugins:
  source:
    - module: databroker
sources:
  xyz:
    description: Some imaginary beamline
    driver: databroker._drivers.jsonl.BlueskyJSONLCatalog
    container: catalog
    args:
      paths: {data_dir / '*.jsonl'}
      handler_registry:
        NPY_SEQ: ophyd.sim.NumpySeqHandler
    metadata:
      beamline: "00-ID"
""")

    def teardown():
        tmp_dir.cleanup()

    db = v1.Broker.from_config({'uri': catalog_path, 'source': 'xyz'})
    serializer = None
    request.addfinalizer(teardown)

    def insert(name, doc):
        nonlocal serializer
        if name == 'start':
            serializer = suitcase.jsonl.Serializer(data_dir, flush=True)
        serializer(name, doc)
        if name == 'stop':
            serializer.close()
            db._catalog.force_reload()

    db.insert = insert
    return db


def build_intake_mongo_backed_broker(request):
    tmp_dir = tempfile.TemporaryDirectory()
    tmp_path = tmp_dir.name
    catalog_path = Path(tmp_path) / 'catalog.yml'
    box = mongobox.MongoBox()
    box.start()
    client = box.client()
    with open(catalog_path, 'w') as file:
        file.write(f"""
plugins:
  source:
    - module: databroker
sources:
  xyz:
    description: Some imaginary beamline
    driver: databroker._drivers.mongo_normalized.BlueskyMongoCatalog
    container: catalog
    args:
      metadatastore_db: mongodb://{client.address[0]}:{client.address[1]}/mds
      asset_registry_db: mongodb://{client.address[0]}:{client.address[1]}/assets
      handler_registry:
        NPY_SEQ: ophyd.sim.NumpySeqHandler
    metadata:
      beamline: "00-ID"
""")

    def teardown():
        "Delete temporary MongoDB data directory."
        box.stop()
        tmp_dir.cleanup()

    request.addfinalizer(teardown)
    db = v1.Broker.from_config({'uri': catalog_path, 'source': 'xyz'})
    serializer = None

    def insert(name, doc):
        nonlocal serializer
        if name == 'start':
            if serializer is not None:
                # serializer.close()
                ...
            serializer = suitcase.mongo_normalized.Serializer(client['mds'],
                                                           client['assets'])
        serializer(name, doc)
        if name == 'stop':
            db._catalog.reload()

    db.insert = insert
    return db

def build_intake_mongo_embedded_backed_broker(request):
    tmp_dir = tempfile.TemporaryDirectory()
    tmp_path = tmp_dir.name
    catalog_path = Path(tmp_path) / 'catalog.yml'
    box = mongobox.MongoBox()
    box.start()
    client = box.client()
    with open(catalog_path, 'w') as file:
        file.write(f"""
plugins:
  source:
    - module: databroker
sources:
  xyz:
    description: Some imaginary beamline
    driver: databroker._drivers.mongo_embedded.BlueskyMongoCatalog
    container: catalog
    args:
      datastore_db: mongodb://{client.address[0]}:{client.address[1]}/permanent
      handler_registry:
        NPY_SEQ: ophyd.sim.NumpySeqHandler
    metadata:
      beamline: "00-ID"
""")

    def teardown():
        "Delete temporary MongoDB data directory."
        box.stop()
        tmp_dir.cleanup()

    request.addfinalizer(teardown)
    db = v1.Broker.from_config({'uri': catalog_path, 'source': 'xyz'})
    serializer = None

    def insert(name, doc):
        nonlocal serializer
        if name == 'start':
            if serializer is not None:
                # serializer.close()
                ...
            serializer = suitcase.mongo_embedded.Serializer(client['permanent'])
        serializer(name, doc)
        if name == 'stop':
            db._catalog.reload()

    db.insert = insert
    return db


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

    tempdirname = tempfile.mkdtemp()
    mds = MDS({'directory': tempdirname,
               'timezone': tzlocal.get_localzone().zone,
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


def build_pymongo_backed_broker(request):
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
