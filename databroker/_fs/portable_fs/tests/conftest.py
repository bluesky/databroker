import uuid
import os
import pytest
import numpy as np
from databroker.resource_registry import sqlite as sqlfs
import tempfile
from databroker.resource_registry.tests.utils import SynHandlerMod


@pytest.fixture(params=[sqlfs], scope='function')
def fs(request):
    '''Provide a function level scoped FileStore instance talking to
    temporary database on localhost:27017 with both v0 and v1.

    '''
    tf = tempfile.NamedTemporaryFile()
    fs = request.param.FileStoreMoving({'dbpath': tf.name}, version=1)
    fs.register_handler('syn-mod', SynHandlerMod)

    def delete_dm():
        os.remove(tf.name)

    request.addfinalizer(delete_dm)

    return fs


@pytest.fixture(params=[sqlfs], scope='function')
def fs_v1(request):
    return fs(request)
