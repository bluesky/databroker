import uuid
import pytest
from metadatastore.mds import MDS


@pytest.fixture(params=[1], scope='function')
def mds_all(request):
    '''Provide a function level scoped FileStore instance talking to
    temporary database on localhost:27017 with both v0 and v1.
    '''
    db_name = "mds_testing_disposable_{}".format(str(uuid.uuid4()))
    test_conf = dict(database=db_name, host='localhost',
                     port=27017, timezone='US/Eastern',
                     mongo_user='tom',
                     mongo_pwd='jerry')
    ver = request.param
    mds = MDS(test_conf, ver)

    def delete_dm():
        print("DROPPING DB")
        mds._connection.drop_database(db_name)

    request.addfinalizer(delete_dm)

    return mds


@pytest.fixture(params=[1], scope='module')
def mds_all_mod(request):
    '''Provide a function level scoped FileStore instance talking to
    temporary database on localhost:27017 with both v0 and v1.
    '''
    db_name = "mds_testing_disposable_{}".format(str(uuid.uuid4()))
    test_conf = dict(database=db_name, host='localhost',
                     port=27017, timezone='US/Eastern',
                     mongo_user='tom',
                     mongo_pwd='jerry')
    ver = request.param

    mds = MDS(test_conf, ver)

    def delete_dm():
        print("DROPPING DB")
        mds._connection.drop_database(db_name)

    request.addfinalizer(delete_dm)

    return mds
