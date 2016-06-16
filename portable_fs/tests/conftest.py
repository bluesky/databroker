import os
import pytest
from filestore.utils import create_test_database
import portable_fs.sqlite.fs
import filestore
import tempfile
from filestore.test.utils import SynHandlerMod


@pytest.fixture(params=[portable_fs.sqlite.fs], scope='function')
def fs(request):
    '''Provide a function level scoped FileStore instance talking to
    temporary database on localhost:27017 with both v0 and v1.

    '''
    tf = tempfile.NamedTemporaryFile()
    fs = request.param.FileStore({'dbpath': tf.name}, version=1)
    fs.register_handler('syn-mod', SynHandlerMod)

    def delete_dm():
        os.remove(tf.name)

    request.addfinalizer(delete_dm)

    return fs
