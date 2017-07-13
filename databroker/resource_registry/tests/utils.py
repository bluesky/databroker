from __future__ import (unicode_literals, print_function, division,
                        absolute_import)
import uuid
import itertools
from ..handlers_base import HandlerBase
import numpy as np
from ..utils import install_sentinels


conn = None
db_name = "fs_testing_disposable_{}".format(str(uuid.uuid4()))
old_conf = None


def fs_setup():
    "Create a fresh database with unique (random) name."
    from ..api import db_connect, db_disconnect
    from .. import conf as fconf

    global conn
    global old_conf

    old_conf = dict(fconf.connection_config)
    db_disconnect()
    test_conf = dict(database=db_name, host='localhost',
                     port=27017)
    install_sentinels(test_conf, 1)
    conn = db_connect(**test_conf)
    old_conf.clear()
    old_conf.update(fconf.connection_config)
    fconf.connection_config.clear()
    fconf.connection_config.update(test_conf)


def fs_teardown():
    "Drop the fresh database and disconnect."
    from ..api import db_disconnect
    from .. import conf as fconf

    conn.drop_database(db_name)
    db_disconnect()
    fconf.connection_config.clear()
    fconf.connection_config.update(old_conf)


class SynHandlerMod(HandlerBase):
    """
    A handler for synthetic data which will return a ramp % n reshaped
    to the frame size for frame n

    Parameters
    ----------
    shape : tuple
        The shape of the frame
    """
    def __init__(self, fpath, shape):
        self._shape = tuple(int(v) for v in shape)
        self._N = np.prod(self._shape)

    def __call__(self, n):
        return np.mod(np.arange(self._N), n).reshape(self._shape)


class SynHandlerEcho(HandlerBase):
    """
    A handler for synthetic data which will return a constant field
    of value `n` of the given frame siz.

    Parameters
    ----------
    shape : tuple
        The shape of the frame
    """
    def __init__(self, fpath, shape):
        self._shape = tuple(int(v) for v in shape)
        self._N = np.prod(self._shape)

    def __call__(self, n):
        return np.ones(self._shape) * n


def insert_syn_data(fs, f_type, shape, count):
    ret, _ = insert_syn_data_with_resource(fs, f_type, shape, count)
    return ret


def insert_syn_data_with_resource(fs, f_type, shape, count):
    fb = fs.insert_resource(f_type, None, {'shape': shape})
    ret = []
    res_map_cycle = itertools.cycle((lambda x: x,
                                     lambda x: x['id'],
                                     lambda x: str(x['id'])))
    for k, rmap in zip(range(count), res_map_cycle):
        r_id = str(uuid.uuid4())
        fs.insert_datum(rmap(fb), r_id, {'n': k + 1})
        ret.append(r_id)
    return ret, fb


def insert_syn_data_bulk(fs, f_type, shape, count):
    fb = fs.insert_resource(f_type, None, {'shape': shape})
    d_uid = [str(uuid.uuid4()) for k in range(count)]
    d_kwargs = [{'n': k + 1} for k in range(count)]
    fs.bulk_insert_datum(fb, d_uid, d_kwargs)

    return d_uid
