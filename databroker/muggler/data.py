# ######################################################################
# Copyright (c) 2014, Brookhaven Science Associates, Brookhaven        #
# National Laboratory. All rights reserved.                            #
#                                                                      #
# Redistribution and use in source and binary forms, with or without   #
# modification, are permitted provided that the following conditions   #
# are met:                                                             #
#                                                                      #
# * Redistributions of source code must retain the above copyright     #
#   notice, this list of conditions and the following disclaimer.      #
#                                                                      #
# * Redistributions in binary form must reproduce the above copyright  #
#   notice this list of conditions and the following disclaimer in     #
#   the documentation and/or other materials provided with the         #
#   distribution.                                                      #
#                                                                      #
# * Neither the name of the Brookhaven Science Associates, Brookhaven  #
#   National Laboratory nor the names of its contributors may be used  #
#   to endorse or promote products derived from this software without  #
#   specific prior written permission.                                 #
#                                                                      #
# THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS  #
# "AS IS" AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT    #
# LIMITED TO, THE IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS    #
# FOR A PARTICULAR PURPOSE ARE DISCLAIMED. IN NO EVENT SHALL THE       #
# COPYRIGHT HOLDER OR CONTRIBUTORS BE LIABLE FOR ANY DIRECT,           #
# INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES   #
# (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR   #
# SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS INTERRUPTION)   #
# HOWEVER CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN CONTRACT,  #
# STRICT LIABILITY, OR TORT (INCLUDING NEGLIGENCE OTHERWISE) ARISING   #
# IN ANY WAY OUT OF THE USE OF THIS SOFTWARE, EVEN IF ADVISED OF THE   #
# POSSIBILITY OF SUCH DAMAGE.                                          #
########################################################################
from __future__ import (absolute_import, division, print_function,
                        unicode_literals)
import six
from collections import namedtuple, OrderedDict, deque
import warnings
import pandas as pd
import numpy as np
from scipy.interpolate import interp1d, interp2d
from pims.base_frames import FramesSequence
from pims.frame import Frame

from skimage.io import imread


class BinningError(Exception):
    """
    An exception to raise if there are insufficient sampling rules to
    upsampling or downsample a data column into specified bins.
    """
    pass


class ColSpec(namedtuple(
              'ColSpec', ['name', 'ndim', 'upsample', 'downsample'])):
    """
    Named-tuple sub-class to validate the column specifications for the
    DataMuggler

    Parameters
    ----------
    name : hashable
    ndim : uint
        Dimensionality of the data stored in the column
    upsample : {None, 'linear', 'nearest', 'zero', 'slinear', 'quadratic',
                'cubic'}
        None means that each time bin must have at least one value.
        The names refer to kinds of scipy.interpolator. See documentation
        link below.
    downsample : {None, 'linear', 'nearest', 'zero', 'slinear', 'quadratic',
                  'cubic'}
        None means that each time bin must have no more than one value.
        The names refer to kinds of scipy.interpolator. See documentation
        link below.

    References
    ----------
    http://docs.scipy.org/doc/scipy/reference/generated/scipy.interpolate.interp1d.html
    """
    # These reflect the 'method' argument of pandas.DataFrame.fillna
    upsampling_methods = {None, 'linear', 'nearest', 'zero', 'slinear', 
                        'quadratic', 'cubic'}

    __slots__ = ()

    def __new__(cls, name, ndim, upsample, downsample):
        # sanity check ndim
        if int(ndim) < 0:
            raise ValueError("ndim must be positive not {}".format(ndim))


        # TODO The upsampling method could be any callable.

        # Validate upsampling method
        if not (upsample in cls.upsampling_methods):
            raise ValueError("{} is not a valid upsampling method. It "
                             "must be one of {}".format(
                             sample, cls.upsampling_methods))

        # TODO The downsampling methods could have string aliases like 'mean'.

        # Validate downsampling method
        if (downsample is not None) and (not callable(downsample)):
            raise ValueError("The downsampling method must be a callable.")

        # pass everything up to base class
        return super(ColSpec, cls).__new__(
            cls, name, ndim, upsample, downsample)


class DataMuggler(object):
    """
    This class provides a wrapper layer of signals and slots
    around a pandas DataFrame to make plugging stuff in for live
    view easier.

    The data collection/event model being used is all measurements
    (that is values that come off of the hardware) are time stamped
    to ring time.

    The language being used through out is that of pandas data frames.

    The data model is that of a sparse table keyed on time stamps which
    is 'densified' on demand by propagating measurements forwards.  Not
    all measurements (ex images) can be filled.  This behavior is controlled
    by the `col_info` tuple.


    Parameters
    ----------
    events : list
        list of Events (any object with the expected attributes will do)
    """
    def __init__(self, events=None):
        self.sources = {}
        self.col_specs = {}
        self._data = deque()
        self._time = deque()
        self._stale = True
        if events is not None:
            for event in events:
                self.append_event(event)

    @classmethod
    def from_tuples(cls, event_tuples, sources=None):
        """
        Parameters
        ----------
        event_tuples : list of (time, data_dict) tuples
            formatted like
            [(<time>: {<data_key>: <value>, <data_key>: <value>, ...}), ...]
        metatdata : dict
            mapping data keys to source names

            This information is used to look up resampling behavior.
        """
        raise NotImplementedError()
        for event in event_tuples:
            # TODO Make this look like an event object.
            pass

    @classmethod
    def from_events(cls, events):
        """
        Parameters
        ----------
        events : list
            list of Events (any object with the expected attributes will do)
        """
        return cls(events)

    def append_event(self, event):
        self._stale = True
        for name, description in event.descriptor.data_keys.items():

            # If we have this source name, check for name collisions.
            if name in self.sources:
                if self.sources[name] != description['source']:
                    raise ValueError("In a previously loaded event, "
                                     "'{0}' refers to {1} but in Event "
                                     "{2} it refers to {3}.".format(
                                         name, self.sources[name],
                                         event.id,
                                         description['source']))

            # If it is a new name, determine a ColSpec.
            else:
                self.sources[name] = description['source']
                if 'external' in event.descriptor.data_keys.keys():
                    # TODO Figure out the specific dimension.
                    pass
                else:
                    dim = 0

                col_spec = ColSpec(name, dim, None, None)  # defaults
                # TODO Look up source-specific default in a config file
                # or some other source of reference data.
                # For now...
                if dim == 0:
                    col_spec = ColSpec(name, dim, 'linear', np.mean)
                else:
                    col_spec = ColSpec(name, dim, None, np.sum)
                self.col_specs[name] = col_spec

            # Both scalar and nonscalar data will get stored in the DataFrame.
            # This may be optimized later, but it might not actually help much.
            self._data.append({name: event.data[name]['value']})
            self._time.append(event.time)

    @property
    def _dataframe(self):
        # Rebuild the DataFrame if more data has been added.
        if self._stale:
            index = pd.Float64Index(list(self._time))
            self._df = pd.DataFrame(list(self._data), index)
            self._stale = False
        return self._df

    def bin_by_edges(self, bin_edges, interpolation=None, agg=None):
        """
        Return data, resampled as necessary.

        Parameters
        ----------
        bin_edges : list
           list of two-element items like [(t1, t2), (t3, t4), ...]

        Returns
        -------
        data : dict of lists
        """
        time = np.array(self._time)
        binning = np.zeros(len(time), dtype=np.bool)
        for i, pair in enumerate(bin_edges):
            binning[(time < pair[0]) & (time > pair[1])] = i
        time_points = [np.mean(pair) for pair in bin_edges]  # bin centers
        return self.resample(time_points, binning, interpolation, agg)

    def resample(self, time_points, binning, interpolation=None, agg=None):

        # How many (non-null) data points in each bin?
        grouped = self._dataframe.groupby(binning)
        counts = grouped.count()

        # Where upsampling is possible, interpolate to the center of each bin.
        # Where not possible, check that there is at least one point per bin.
        # If there is not, raise.
        resampled_df = pd.DataFrame(index=time_points)
        for col_name in self._dataframe:
            col_spec = self.col_specs[col_name]
            if interpolation is not None:
                upsample = interpolation  # TODO validation
            else:
                upsample = col_spec.upsample
            if upsample is not None:
                # TODO This logic should be higher up, but that might require
                # breaking the namedtuple idea.
                if col_spec.ndim != 0:
                    raise NotImplementedError("Can't upsample nonscalar data")
                dense_col = self._dataframe[col_name].dropna()
                x, y = dense_col.index.values, dense_col.values
                def curried_interpolator(new_x):
                    return interp1d(x, y, kind=col_spec.upsample)
                resampled_df[col_name] = curried_interpolator(time_points)
                # There is now exactly one point per bin.
                continue
            else:
                if np.any(counts[col] < 1):
                    raise BinningError("The specified binning leaves some "
                                       "bins without '{0}' data, and there is "
                                       "no rule for interpolating it between "
                                       "bins.".format(col_name))

            # Columns that could be interpolate (above) are done. For the rest,
            # if they have one data point per bin, we are done. If not, the
            # multi-valued bins must be downsampled (reduced). If there is no
            # rule for downsampling, raise.
            if np.all(counts[col_name]) == 1:
                continue
            if agg is not None:
                downsample = agg  # TODO validation
            else:
                downsample = colspec.downsample
            if downsample is None:
                raise BinningError("The specified binning puts multiple '{0}'"
                                   "measurements in at least one bin, and "
                                   "there is no rule for downsampling "
                                   "(i.e., reducing) it.".format(col_name))
            resampled_df[col_name] = grouped.agg(
                {col_name: col_spec.downsample})

    @property
    def col_dims(self):
        """
        The dimensionality of the data stored in all columns. Returned as a
        dictionary keyed on column name.

         0 -> scalar
         1 -> line (MCA spectra)
         2 -> image
         3 -> volume
        """
        return {c.name: c.dims for c in self._col_info}

    @property
    def ncols(self):
        """
        The number of columns that the DataMuggler contains
        """
        return len(self._col_info)

    @property
    def col_fill_rules(self):
        """
        Fill rules for all of the columns.
        """
        return {c.name: c.fill_method for c in self._col_info}

    def align_against(self, ref_col, other_cols=None):
        """
        DEPRECATED: For backward-compatibility, the returns a dict with all
        values True.
        """
        warnings.warn("align_against is deprecated; everything can be aligned")
        dict_of_truth = {col_name: True for col_name in self.keys()}
        return dict_of_truth

    def keys(self, dim=None):
        """
        Get the column names in the data muggler

        Parameters
        ----------
        dim : int
            Select out only columns with the given dimensions

            --  ------------------
            0   scalar
            1   line (MCA spectra)
            2   image
            3   volume
            --  -------------------

        Returns
        -------
        keys : list
            Column names in the data muggler that match the desired
            dimensionality, or all column names if dim is None
        """
        cols = [c.name for c in self._col_info
                if (True if dim is None else dim == c.dims)]
        cols.sort(key=lambda s: s.lower())
        return cols

    def _listify_output(self, df):
        """
        Turn a DataFrame into a dict of lists.

        This does very little validation as it is an internal function.

        This is intended to be used _after_ the data frame has been reduced
        to only the rows where the reference column has values.

        Parameters
        ----------
        df : DataFrame
            This needs to be a densified version the `_dataframe` possibly
            with a reduced number of columns.

        Returns
        -------
        index : ndarray
            The index of the data frame
        data : dict
            Dictionary keyed on column name of the column.  The value is
            one of (ndarray, list, pd.Series)
        """
        df = self._dataframe  # for brevity
        dict_of_lists = {col: df[col].to_list() for col in df.columns}
        return df.index.values, dict_of_lists
