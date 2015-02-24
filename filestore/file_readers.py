from __future__ import (absolute_import, division, print_function,
                        unicode_literals)
from .retrieve import HandlerBase

import six
import logging
import h5py
import numpy as np
import os.path

logger = logging.getLogger(__name__)


class _HDF5HandlerBase(HandlerBase):

    def open(self):
        if self._file:
            return
        self._file = h5py.File(self._filename)

    def close(self):
        super(HDF5HandlerBase, self).close()
        self._file.close()


class HDF5DatasetSliceHandler(_HDF5HandlerBase):
    "Handler for Stuart's first detector demo"
    def __init__(self, filename, frame_per_point):
        self._filename = filename
        self.open()

    def __call__(self, point_number):
        dataset_name = '/entry/data/data'
        # Don't read out the dataset until it is requested for the first time.
        if not hasattr(self, '_dataset'):
            self._dataset = self._file[dataset_name]
        return self._dataset[point_number, :, :]


class _HdfMapsHandlerBase(_HDF5HandlerBase):
    """
    Reader for XRF data stored in hdf5 files.

    The data set is assumed to be in a group called MAPS and stored
    as a 3D array ordered [energy, x, y].

    Parameters
    ----------
    filename : str
        Path to physical location of file
    dset_path : str
        The path to the dataset inside of 'MAPS'
    """
    def __init__(self, filename, dset_path):
        self._filename = filename
        self._dset_path = dset_path
        self._file = None
        self._dset = None
        self.open()

    def open(self):
        """
        Open the file for reading.

        Provided as a stand alone function to allow re-opening of the handler
        """
        if self._file:
            return
        self._file = h5py.File(self._filename, mode='r')
        self._dset = self._file['/'.join(['MAPS', self._dset_path])]

    def close(self):
        """
        Close the underlying file
        """
        super(_HdfMapsHandlerBase, self).close()
        self._file.close()

    def __call__(self):

        if not self._file:
            raise RuntimeError("File is not open")


class HDFMapsSpectrumHandler(_HdfMapsHandlerBase):
    def __call__(self, x, y):
        """
        Return the spectrum at the x, y position

        Parameters
        ----------
        x : int
            raster index in the x direction

        y : int
            raster index in the y direction

        Returns
        -------
        spectrum : ndarray
            The MCA channels
        """
        super(HDFMapsSpectrumHandler, self).__call__()
        return self._dset[:, x, y]


class HDFMapsEnergyHandler(_HdfMapsHandlerBase):
    def __call__(self, e_index):
        """
        Return the raster plane at a fixed energy

        Parameters
        ----------
        e_index : int
            The index of the engery

        Returns
        -------
        plane : ndarray
            The raster image at a fixed energy.
        """
        super(HDFMapsEnergyHandler, self).__call__()
        return self._dset[e_index, :, :]


class NpyHandler(HandlerBase):
    """
    Class to deal with reading npy files

    Parameters
    ----------
    fpath : str
        Path to file

    mmap_mode : {'r', 'r+', c}, optional
        memmap mode to use to open file
    """
    def __init__(self, filename, mmap_mode=None):
        self._mmap_mode = mmap_mode
        if not os.path.exists(filename):
            raise IOError("the requested file {fpath} does not exst")
        self._fpath = filename

    def __call__(self):
        return np.load(self._fpath, self._mmap_mode)
