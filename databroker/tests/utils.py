import os
import shutil
import tempfile
import uuid
import time

import tzlocal

from databroker import Broker, BrokerES, temp_config
from databroker.headersource import HeaderSourceShim
from databroker.eventsource import EventSourceShim
from subprocess import Popen


def build_sqlite_backed_broker(request):
    """Uses mongoquery + sqlite -- no pymongo or mongo server anywhere"""

    config = temp_config()
    tempdir = config['metadatastore']['config']['directory']

    def cleanup():
        shutil.rmtree(tempdir)

    request.addfinalizer(cleanup)

    return Broker.from_config(config)


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

    return BrokerES(HeaderSourceShim(mds),
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

    return Broker(mds, fs)


def start_md_server(testing_config):
    cmd = ["start_md_server", "--mongo-host",
           testing_config["mongohost"],
           "--mongo-port",
           str(testing_config['mongoport']),
           "--database", testing_config['database'],
           "--timezone", testing_config['tzone'],
           "--service-port",
           str(testing_config['serviceport'])]
    print(' '.join(cmd))
    proc = Popen(cmd)
    print('Started the server with configuration..:{}'.format(testing_config))
    return proc


def stop_md_server(proc, testing_config):

    from pymongo import MongoClient
    Popen(['kill', '-9', str(proc.pid)])
    conn = MongoClient(host=testing_config['mongohost'],
                       port=testing_config['mongoport'])
    conn.drop_database(testing_config['database'])


def build_client_backend_broker(request):
    from ..headersource.client import MDS
    from ..assets.utils import create_test_database
    from ..assets.mongo import Registry
    import requests.exceptions
    from random import randint
    import ujson

    port = randint(9000, 60000)
    testing_config = dict(mongohost='localhost', mongoport=27017,
                          database='mds_test'+str(uuid.uuid4()),
                          serviceport=port, tzone='US/Eastern')

    proc = start_md_server(testing_config)

    tmds = MDS({'host': 'localhost',
                'port': port,
                'timezone': 'US/Eastern'})
    db_name = "fs_testing_base_disposable_{uid}"
    fs_test_conf = create_test_database(host='localhost',
                                        port=27017, version=1,
                                        db_template=db_name)
    fs = Registry(fs_test_conf)

    def tear_down():
        stop_md_server(proc, testing_config)

    request.addfinalizer(tear_down)

    base_url = 'http://{}:{}/'.format('localhost',
                                      testing_config['serviceport'])
    # Wait here until the server responds. Time out after 1 minute.
    TIMEOUT = 60  # seconds
    startup_time = time.time()
    url = base_url + 'run_start'
    message = dict(query={}, signature='find_run_starts')
    print("Waiting up to 60 seconds for the server to start up....")
    while True:
        if time.time() - startup_time > TIMEOUT:
            raise Exception("Server startup timed out.")
        try:
            r = requests.get(url, params=ujson.dumps(message))
        except requests.exceptions.ConnectionError:
            time.sleep(1)
            continue
        else:
            break
    print("Server is up!")

    return Broker(tmds, fs)
