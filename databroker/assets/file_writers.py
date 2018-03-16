from __future__ import (absolute_import, division, print_function,
                        unicode_literals)
from .handlers_base import HandlerBase

import errno
import six
import logging
import numpy as np
import uuid
import os
import os.path as op
import datetime

from ..utils import ensure_path_exists

logger = logging.getLogger(__name__)


class NpyWriter(HandlerBase):
    """
    Class to handle writing a numpy array out to disk and registering
    that write with Registry.

    This class is only good for one call to add_data.

    Parameters
    ----------
    fpath : str
        Path (including filename) of where to save the file

    resource_kwargs : dict, optional
        Saved in the resource_kwargs field of the fileBase document.  Valid
        keys are {mmap_mode, }
    """

    SPEC_NAME = 'npy'

    def __init__(self, fpath, fs, resource_kwargs=None):
        if op.exists(fpath):
            raise IOError("the requested file {fpath} already exist")
        self._fpath = fpath
        if resource_kwargs is None:
            resource_kwargs = dict()
        for k in resource_kwargs.keys():
            if k != 'mmap_mode':
                raise ValueError("The only valid resource_kwargs key is "
                                 "'mmap_mode' "
                                 "you passed in {}".format(k))
        self._f_custom = dict(resource_kwargs)

        self._writable = True
        self.fs = fs

    def add_data(self, data, uid=None, resource_kwargs=None):
        """
        Parameters
        ----------
        data : ndarray
            The data to save

        uid : str, optional
            The uid to be used for this entry,
            if not given use uuid1 to generate one

            .. warning ::

               This may only be taken as a suggestion.

        resource_kwargs : None, optional
            Currently raises if not 'falsy' and is ignored.

        Returns
        -------
        uid : str
            The uid used to register this data with an asset registry, can
            be used to retrieve it
        """
        if not self._writable:
            raise RuntimeError("This writer can only write one data entry "
                               "and has already been used")

        if resource_kwargs:
            raise ValueError("This writer does not support resource_kwargs")

        if op.exists(self._fpath):
            raise IOError("the requested file {fpath} "
                          "already exist".format(fpath=self._fpath))

        np.save(self._fpath, np.asanyarray(data))
        self._writable = False
        fb = self.fs.insert_resource(self.SPEC_NAME,
                                     self._fpath,
                                     self._f_custom,
                                     root='/')
        if uid is None:
            uid = fb['uid'] + '/0'
        evl = self.fs.insert_datum(fb, uid, {})

        return evl['datum_id']


def save_ndarray(data, fs, base_path=None, filename=None):
    """
    Helper method to mindlessly save a numpy array to disk.

    Defaults to saving files in :path:`~/.fs_cache/YYYY-MM-DD`


    Parameters
    ----------
    data : ndarray
        The data to be saved

    base_path : str, optional
        The base-path to use for saving files.  If not given
        default to `~/.fs_cache`.  Will add a sub-directory for
        each day in this path.

    filename : str, optional
        The name of the file to save to disk.  Defaults to a uuid4 if none is
        given
    """
    if base_path is None:
        xdg_data = os.getenv('XDG_DATA_HOME')
        if not xdg_data:
            xdg_data = op.join(op.expanduser('~'), '.local', 'share')
        base_path = op.join(xdg_data, 'fs_cache',
                            str(datetime.date.today()))
    if filename is None:
        filename = str(uuid.uuid4())
    ensure_path_exists(base_path)
    fpath = op.join(base_path, filename + '.npy')
    with NpyWriter(fpath, fs) as fout:
        datum_id = fout.add_data(data)

    return datum_id
