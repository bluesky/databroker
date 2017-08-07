import pytest
from ..utils import create_test_database

from databroker.resource_registry import mongo as ffs


@pytest.fixture(scope='function')
def fs(request):
    '''Provide a function level scoped FileStore instance talking to
    temporary database on localhost:27017 with v1.

    '''
    from .utils import SynHandlerMod
    db_name = "fs_testing_base_disposable_{uid}"
    test_conf = create_test_database(host='localhost',
                                     port=27017, version=1,
                                     db_template=db_name)
    fs = ffs.FileStore(test_conf, version=1)
    fs.register_handler('syn-mod', SynHandlerMod)

    def delete_dm():
        print("DROPPING DB")
        fs._connection.drop_database(test_conf['database'])

    request.addfinalizer(delete_dm)
    return fs


@pytest.fixture(scope='function')
def fs_v1(request):
    '''Provide a function level scoped FileStoreMoving instance talking to
    temporary database on localhost:27017. v1 only

    '''
    db_name = "fs_testing_v1_disposable_{uid}"
    test_conf = create_test_database(host='localhost',
                                     port=27017, version=1,
                                     db_template=db_name)
    fs = ffs.FileStoreMoving(test_conf,
                             version=1)

    def delete_dm():
        print("DROPPING DB")
        fs._connection.drop_database(db_name)

    request.addfinalizer(delete_dm)

    return fs


@pytest.fixture(scope='class')
def fs_cls(request):
    '''Provide a function level scoped FileStore instance talking to
    temporary database on localhost:27017 with v1.

    '''
    from .utils import SynHandlerMod
    db_name = "fs_testing_base_disposable_{uid}"
    test_conf = create_test_database(host='localhost',
                                     port=27017, version=1,
                                     db_template=db_name)
    fs = ffs.FileStore(test_conf, version=1)
    fs.register_handler('syn-mod', SynHandlerMod)

    def delete_dm():
        print("DROPPING DB")
        fs._connection.drop_database(test_conf['database'])

    request.addfinalizer(delete_dm)
    return fs
