import pytest
from filestore.utils import create_test_database
from .utils import (SynHandlerMod, install_sentinels)
import filestore.fs
import uuid


@pytest.fixture(params=[0, 1], scope='function')
def fs(request):
    '''Provide a function level scoped FileStore instance talking to
    temporary database on localhost:27017 with both v0 and v1.

    '''
    version = request.param
    db_name = "fs_testing_base_disposable_{uid}"
    test_conf = create_test_database(host='localhost',
                                     port=27017, version=version,
                                     db_template=db_name)
    fs = filestore.fs.FileStore(test_conf, version=version)
    fs.register_handler('syn-mod', SynHandlerMod)

    def delete_dm():
        print("DROPPING DB")
        fs._connection.drop_database(test_conf['database'])

    request.addfinalizer(delete_dm)
    return fs


@pytest.fixture(scope='function')
def fs_v01(request):
    '''Provide a function level scoped FileStore instance talking to
    temporary database on localhost:27017 with both v0 and v1.

    '''
    db_name = "fs_testing_v01_disposable_{}".format(str(uuid.uuid4()))
    test_conf = dict(database=db_name, host='localhost',
                     port=27017)
    # v0 does not check!
    install_sentinels(test_conf, 1)
    fs0 = filestore.fs.FileStore(test_conf, version=0)
    fs1 = filestore.fs.FileStoreMoving(test_conf, version=1)

    fs0.register_handler('syn-mod', SynHandlerMod)
    fs1.register_handler('syn-mod', SynHandlerMod)

    def delete_dm():
        print("DROPPING DB")
        fs1._connection.drop_database(db_name)

    request.addfinalizer(delete_dm)

    return fs0, fs1


@pytest.fixture(scope='function')
def fs_v1(request):
    '''Provide a function level scoped FileStore instance talking to
    temporary database on localhost:27017. v1 only

    '''
    db_name = "fs_testing_v1_disposable_{}".format(str(uuid.uuid4()))
    test_conf = dict(database=db_name, host='localhost',
                     port=27017)
    install_sentinels(test_conf, 1)
    fs = filestore.fs.FileStoreMoving(test_conf,
                                      version=1)

    def delete_dm():
        print("DROPPING DB")
        fs._connection.drop_database(db_name)

    request.addfinalizer(delete_dm)

    return fs
