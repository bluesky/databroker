import pytest

from ..utils import create_test_database


def mongo_fs_factory():
    from databroker.assets import mongo as ffs
    db_name = "fs_testing_base_disposable_{uid}"
    test_conf = create_test_database(host='localhost',
                                     port=27017, version=1,
                                     db_template=db_name)
    fs = ffs.RegistryMoving(test_conf)

    def delete_dm():
        print("DROPPING DB")
        fs._connection.drop_database(test_conf['database'])

    return fs, delete_dm


def sqlite_fs_factory():
    from databroker.assets import sqlite as sqlfs
    import tempfile
    import os
    tf = tempfile.NamedTemporaryFile()
    fs = sqlfs.RegistryMoving({'dbpath': tf.name})

    def delete_dm():
        os.remove(tf.name)

    return fs, delete_dm


def hdf5_fs_factory():
    from databroker.assets import column_hdf5 as chdf5
    import tempfile
    import shutil

    tp = tempfile.mkdtemp()
    fs = chdf5.RegistryMoving({'dbpath': tp})

    def cleanup():
        shutil.rmtree(tp)

    return fs, cleanup


def _use_factory(request):
    from .utils import SynHandlerMod
    factory = request.param
    fs, delete_dm = factory()
    fs.register_handler('syn-mod', SynHandlerMod)

    request.addfinalizer(delete_dm)
    return fs


@pytest.fixture(scope='function', params=[mongo_fs_factory, sqlite_fs_factory,
                                          hdf5_fs_factory],
                ids=['mongo', 'sqlite', 'column_hdf5'])
def fs(request):
    '''Provide a function level scoped Registry instance talking to
    temporary database on localhost:27017 with v1.

    '''
    return _use_factory(request)


registry = fs


@pytest.fixture(scope='function', params=[mongo_fs_factory])
def fs_mongo(request):
    '''Provide a function level scoped Registry instance talking to
    temporary database on localhost:27017 with v1.

    '''
    return _use_factory(request)


fs_v1 = fs


@pytest.fixture(scope='class', params=[mongo_fs_factory, sqlite_fs_factory],
                ids=['mongo', 'sqlite'])
def fs_cls(request):
    '''Provide a function level scoped Registry instance talking to
    temporary database on localhost:27017 with v1.

    '''
    return _use_factory(request)
