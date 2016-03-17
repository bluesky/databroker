from __future__ import (unicode_literals, print_function, division,
                        absolute_import)
import uuid
from filestore.api import db_connect, db_disconnect
import filestore.conf as fconf

from filestore.handlers_base import HandlerBase
import numpy as np


conn = None
db_name = "fs_testing_disposable_{}".format(str(uuid.uuid4()))
old_conf = dict(fconf.connection_config)


def fs_setup():
    "Create a fresh database with unique (random) name."
    global conn
    db_disconnect()
    test_conf = dict(database=db_name, host='localhost',
                     port=27017)
    conn = db_connect(**test_conf)
    old_conf.clear()
    old_conf.update(fconf.connection_config)
    fconf.connection_config.clear()
    fconf.connection_config.update(test_conf)


def fs_teardown():
    "Drop the fresh database and disconnect."
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
