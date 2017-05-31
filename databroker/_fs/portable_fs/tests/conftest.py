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


@pytest.fixture()
def moving_files(fs_v1, tmpdir):
    tmpdir = str(tmpdir)
    cnt = 15
    shape = (7, 13)

    local_path = '2016/04/28/aardvark'
    fmt = 'cub_{point_number:05}.npy'
    res = fs_v1.insert_resource('npy_series',
                                local_path,
                                {'fmt': fmt},
                                root=tmpdir)
    datum_uids = []
    fnames = []
    os.makedirs(os.path.join(tmpdir, local_path))
    for j in range(cnt):
        fpath = os.path.join(tmpdir, local_path,
                             fmt.format(point_number=j))
        np.save(fpath, np.ones(shape) * j)
        d = fs_v1.insert_datum(res, str(uuid.uuid4()),
                               {'point_number': j})
        datum_uids.append(d['datum_id'])
        fnames.append(fpath)

    return fs_v1, res, datum_uids, shape, cnt, fnames
