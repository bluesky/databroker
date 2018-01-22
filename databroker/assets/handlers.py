from __future__ import (absolute_import, division, print_function,
                        unicode_literals)
import logging
import numpy as np
import os.path

from .handlers_base import HandlerBase
from .readers.spe import PrincetonSPEFile
from pims import FramesSequence, Frame

logger = logging.getLogger(__name__)


# The ImageCube class is used for a per event representation of
# a dataset

class ImageStack(FramesSequence):
    "One of these represents the data from an event: (num_images x w x h)"
    def __init__(self, dataset, start, stop):
        # `start` and `stop` are the limits of this cube
        # i indexes within the cube
        self._start = start
        self._stop = stop
        self._dataset = dataset

        # Old PIMS has no dtype, shape. Try adding it.
        try:
            self.dtype = self.pixel_type
        except AttributeError:
            pass
        try:
            self.shape = self.frame_shape
        except AttributeError:
            pass

    def get_frame(self, i):
        return Frame(self._dataset[self._start + i], frame_no=i)

    def __len__(self):
        return self._stop - self._start

    @property
    def pixel_type(self):
        return self._dataset.dtype

    @property
    def frame_shape(self):
        return self._dataset.shape[1:]


class IntegrityError(Exception):
    pass


class AreaDetectorSPEHandler(HandlerBase):
    specs = {'AD_SPE'} | HandlerBase.specs

    def __init__(self, fpath, template, filename,
                 frame_per_point=1):
        self._path = os.path.join(fpath, '')
        self._fpp = frame_per_point
        self._template = template
        self._filename = filename
        self._f_cache = dict()

    def __call__(self, point_number):
        if point_number not in self._f_cache:
            fname = self._template % (self._path,
                                      self._filename,
                                      point_number)
            spe_obj = PrincetonSPEFile(fname)
            self._f_cache[point_number] = spe_obj

        spe = self._f_cache[point_number]
        data = spe.getData()

        if data.shape[0] != self._fpp:
            raise IntegrityError("expected {} frames, found {} frames".format(
                                 self._fpp, data.shape[0]))
        return data.squeeze()

    def get_file_list(self, datum_kwarg_gen):
        return [self._template % (self._path,
                                  self._filename,
                                  d['point_number'])
                for d in datum_kwarg_gen]


class AreaDetectorTiffHandler(HandlerBase):
    specs = {'AD_TIFF'} | HandlerBase.specs

    def __init__(self, fpath, template, filename, frame_per_point=1):
        self._path = os.path.join(fpath, '')
        self._fpp = frame_per_point
        self._template = template
        self._filename = filename

    def _fnames_for_point(self, point_number):
        start = int(point_number * self._fpp)
        stop = int((point_number + 1) * self._fpp)
        for j in range(start, stop):
            yield self._template % (self._path, self._filename, j)

    def __call__(self, point_number):
        import tifffile
        ret = []
        for fn in self._fnames_for_point(point_number):
            with tifffile.TiffFile(fn) as tif:
                ret.append(tif.asarray())
        return np.array(ret).squeeze()

    def get_file_list(self, datum_kwargs):
        ret = []
        for d_kw in datum_kwargs:
            ret.extend(self._fnames_for_point(**d_kw))
        return ret


class DummyAreaDetectorHandler(HandlerBase):
    def __init__(self, fpath, frame_per_point=1, **kwargs):

        self._fpp = frame_per_point

    def __call__(self, **kwargs):
        out_stack = np.ones((self._fpp, 10, 10)) * np.nan
        # return stacked and squeezed results
        return out_stack.squeeze()


class HDF5DatasetSliceHandler(HandlerBase):
    """
    Handler for data stored in one Dataset of an HDF5 file.

    Parameters
    ----------
    filename : string
        path to HDF5 file
    key : string
        key of the single HDF5 Dataset used by this Handler
    frame_per_point : integer, optional
        number of frames to return as one datum, default 1
    swmr : bool, optional
        Open the hdf5 file in SWMR read mode. Only used when mode = 'r'.
        Default is False.
    """
    def __init__(self, filename, key, frame_per_point=1):
        self._fpp = frame_per_point
        self._filename = filename
        self._key = key
        self._file = None
        self._dataset = None
        self._data_objects = {}
        self.open()

    def get_file_list(self, datum_kwarg_gen):
        return [self._filename]

    def __call__(self, point_number):
        # Don't read out the dataset until it is requested for the first time.
        if not self._dataset:
            self._dataset = self._file[self._key]

        if point_number not in self._data_objects:
            start = point_number * self._fpp
            stop = (point_number + 1) * self._fpp
            self._data_objects[point_number] = ImageStack(self._dataset,
                                                          start, stop)
        return self._data_objects[point_number]

    def open(self):
        import h5py
        if self._file:
            return

        self._file = h5py.File(self._filename, 'r')

    def close(self):
        super(HDF5DatasetSliceHandler, self).close()
        self._file.close()
        self._file = None


class AreaDetectorHDF5Handler(HDF5DatasetSliceHandler):
    """
    Handler for the 'AD_HDF5' spec used by Area Detectors.

    In this spec, the key (i.e., HDF5 dataset path) is always
    '/entry/data/data'.

    Parameters
    ----------
    filename : string
        path to HDF5 file
    frame_per_point : integer, optional
        number of frames to return as one datum, default 1
    """
    specs = {'AD_HDF5'} | HDF5DatasetSliceHandler.specs

    def __init__(self, filename, frame_per_point=1):
        hardcoded_key = '/entry/data/data'
        super(AreaDetectorHDF5Handler, self).__init__(
            filename=filename, key=hardcoded_key,
            frame_per_point=frame_per_point)


class AreaDetectorHDF5SWMRHandler(AreaDetectorHDF5Handler):
    """
    Handler for the 'AD_HDF5_SWMR' spec used by Area Detectors.

    In this spec, the key (i.e., HDF5 dataset path) is always
    '/entry/data/data'.

    Parameters
    ----------
    filename : string
        path to HDF5 file
    frame_per_point : integer, optional
        number of frames to return as one datum, default 1
    """
    specs = {'AD_HDF5_SWMR'} | HDF5DatasetSliceHandler.specs

    def open(self):
        import h5py
        if self._file:
            return

        self._file = h5py.File(self._filename, 'r', swmr=True)

    def __call__(self, point_number):
        if self._dataset is not None:
            self._dataset.id.refresh()
        rtn = super(AreaDetectorHDF5SWMRHandler, self).__call__(
            point_number)

        return rtn


class AreaDetectorHDF5TimestampHandler(HandlerBase):
    """ Handler to retrieve timestamps from Areadetector HDF5 File

    In this spec, the timestamps of the images are read.

    Parameters
    ----------
    filename : string
        path to HDF5 file
    frame_per_point : integer, optional
        number of frames to return as one datum, default 1
    """
    specs = {'AD_HDF5_TS'} | HandlerBase.specs

    def __init__(self, filename, frame_per_point=1):
        self._fpp = frame_per_point
        self._filename = filename
        self._key = ['/entry/instrument/NDAttributes/NDArrayEpicsTSSec',
                     '/entry/instrument/NDAttributes/NDArrayEpicsTSnSec']
        self._file = None
        self._dataset1 = None
        self._dataset2 = None
        self.open()

    def __call__(self, point_number):
        # Don't read out the dataset until it is requested for the first time.
        if not self._dataset1:
            self._dataset1 = self._file[self._key[0]]
        if not self._dataset2:
            self._dataset2 = self._file[self._key[1]]
        start, stop = point_number * self._fpp, (point_number + 1) * self._fpp
        rtn = self._dataset1[start:stop].squeeze()
        rtn = rtn + (self._dataset2[start:stop].squeeze() * 1e-9)
        return rtn

    def open(self):
        import h5py
        if self._file:
            return
        self._file = h5py.File(self._filename, 'r')

    def close(self):
        super(AreaDetectorHDF5TimestampHandler, self).close()
        self._file.close()
        self._file = None


class AreaDetectorHDF5SWMRTimestampHandler(AreaDetectorHDF5TimestampHandler):
    """ Handler to retrieve timestamps from Areadetector HDF5 File

    In this spec, the timestamps of the images are read. Reading
    is done using SWMR option to allow read during processing

    Parameters
    ----------
    filename : string
        path to HDF5 file
    frame_per_point : integer, optional
        number of frames to return as one datum, default 1
    """
    specs = {'AD_HDF5_SWMR_TS'} | HandlerBase.specs

    def open(self):
        import h5py
        if self._file:
            return
        self._file = h5py.File(self._filename, 'r', swmr=True)

    def __call__(self, point_number):
        if (self._dataset1 is not None) and (self._dataset2 is not None):
            self._dataset.id.refresh()
        rtn = super(AreaDetectorHDF5SWMRTimestampHandler, self).__call__(
            point_number)
        return rtn


class _HdfMapsHandlerBase(HDF5DatasetSliceHandler):
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
        self._swmr = False
        self.open()

    def open(self):
        """
        Open the file for reading.

        Provided as a stand alone function to allow re-opening of the handler
        """
        super(_HdfMapsHandlerBase, self).open()
        self._dset = self._file['/'.join(['MAPS', self._dset_path])]

    def __call__(self):

        if not self._file:
            raise RuntimeError("File is not open")

        if self._swmr:
            self._dataset.id.refresh()


class HDFMapsSpectrumHandler(_HdfMapsHandlerBase):
    """
    Handler which selects energy spectrum from
    a MAPS XRF data product.
    """
    specs = {'MAPS_SPECTRUM'} | _HdfMapsHandlerBase.specs

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
    """
    Handler which select fixed-energy slices from
    a MAPS XRF data file.
    """
    specs = {'MAPS_PLANE'} | _HdfMapsHandlerBase.specs

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
    specs = {'npy'} | HandlerBase.specs

    def __init__(self, filename, mmap_mode=None):
        self._mmap_mode = mmap_mode
        if not os.path.exists(filename):
            raise IOError("the requested file {fpath} does not exist")
        self._fpath = filename

    def __call__(self):
        return np.load(self._fpath, self._mmap_mode)

    def get_file_list(self, datum_kwarg_gen):
        return [self._fpath]


class NpyFrameWise(HandlerBase):
    specs = {'npy_FRAMEWISE'} | HandlerBase.specs

    def __init__(self, filename, mmap_mode=None):
        self._mmap_mode = mmap_mode
        if not os.path.exists(filename):
            raise IOError("the requested file {fpath} does not exist")
        self._fpath = filename
        self._data = np.load(self._fpath, self._mmap_mode)

    def __call__(self, frame_no):
        return self._data[frame_no]

    def get_file_list(self, datum_kwarg_gen):
        return [self._fpath]


class SingleTiffHandler(HandlerBase):
    specs = {'SINGLE_TIFF'} | HandlerBase.specs

    def __init__(self, filename):
        self._name = filename

    def __call__(self):
        import tifffile
        return tifffile.imread(self._name)


class DATHandler(HandlerBase):
    ''' This handles vague text files through numpy's loadtxt module.
            Useful for the ubiquitous, vague and well loved ".DAT" file.
            Uses defaults from np.loadtxt.

        See Also
        --------
        np.loadtxt
    '''
    specs = {'DAT'} | HandlerBase.specs

    def __init__(self, fpath, **kwargs):
        self._path = fpath
        self.kwargs = kwargs

    def __call__(self):
        return np.loadtxt(self._path, **self.kwargs)


class PilatusCBFHandler(HandlerBase):
    specs = {'AD_CBF'} | HandlerBase.specs

    def __init__(self, rpath, template, filename, frame_per_point=1,
                 initial_number=1):
        self._path = os.path.join(rpath, '')
        self._fpp = frame_per_point
        self._template = template
        self._filename = filename
        self._initial_number = initial_number

    def __call__(self, point_number):
        import fabio
        start, stop = (self._initial_number + point_number *
                       self._fpp, (point_number + 2) * self._fpp)
        ret = []
        # commented out by LY to test scan speed imperovement, 2017-01-24
        for j in range(start, stop):
            fn = self._template % (self._path, self._filename, j)
            img = fabio.open(fn)
            ret.append(img.data)
        return np.array(ret).squeeze()

    def get_file_list(self, datum_kwargs_gen):
        file_list = []
        for dk in datum_kwargs_gen:
            point_number = dk['point_number']
            start, stop = (self._initial_number + point_number *
                           self._fpp, (point_number + 2) * self._fpp)
            for j in range(start, stop):
                fn = self._template % (self._path, self._filename, j)
                file_list.append(fn)
        return file_list


XS3_XRF_DATA_KEY = 'entry/instrument/detector/data'


class Xspress3HDF5Handler(HandlerBase):
    specs = {'XSP3'} | HandlerBase.specs
    HANDLER_NAME = 'XSP3'

    def __init__(self, filename, key=XS3_XRF_DATA_KEY):
        import h5py
        if isinstance(filename, h5py.File):
            self._file = filename
            self._filename = self._file.filename
        else:
            self._filename = filename
            self._file = None
        self._key = key
        self._dataset = None

        self.open()

    def open(self):
        import h5py
        if self._file:
            return

        self._file = h5py.File(self._filename, 'r')

    def close(self):
        super(Xspress3HDF5Handler, self).close()
        if self._file is not None:
            self._file.close()
            self._file = None
        self._dataset = None

    @property
    def dataset(self):
        return self._dataset

    def _get_dataset(self):
        if self._dataset is not None:
            return

        hdf_dataset = self._file[self._key]
        try:
            self._dataset = np.asarray(hdf_dataset)
        except MemoryError as ex:
            logger.warning('Unable to load the full dataset into memory',
                           exc_info=ex)
            self._dataset = hdf_dataset

    def __del__(self):
        try:
            self.close()
        except Exception as ex:
            logger.warning('Failed to close file',
                           exc_info=ex)

    def __call__(self, frame=None, channel=None):
        # Don't read out the dataset until it is requested for the first time.
        self._get_dataset()
        return self._dataset[frame, channel - 1, :].squeeze()

    def get_roi(self, chan, bin_low, bin_high, frame=None, max_points=None):
        self._get_dataset()

        roi = np.sum(self._dataset[:, chan - 1, bin_low:bin_high], axis=1)
        if max_points is not None:
            roi = roi[:max_points]

            if len(roi) < max_points:
                roi = np.pad(roi, ((0, max_points - len(roi)), ), 'constant')

        if frame is not None:
            roi = roi[frame, :]

        return roi

    def get_file_list(self, datum_kwarg_gen):
        return [self._filename]

    def __repr__(self):
        return '{0.__class__.__name__}(filename={0._filename!r})'.format(self)
