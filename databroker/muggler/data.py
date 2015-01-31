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
from pims.base_frames import FramesSequence
from pims.frame import Frame

from skimage.io import imread


class Unalignable(Exception):
    """
    An exception to raise if you try to align a non-fillable column
    to a non-pre-aligned column
    """
    pass


class ColSpec(namedtuple(
              'ColSpec', ['name', 'dims', 'upsample', 'downsmaple'])):
    """
    Named-tuple sub-class to validate the column specifications for the
    DataMuggler

    Parameters
    ----------
    name : hashable
    dims : uint
        Dimensionality of the data stored in the column
    upsample : {None, 'ffill', 'bfill', callable}
        None means that no filling is done
    downsample : {None, 'ffill', 'bfill', callable}
        None means that no filling is done
    """
    # These reflect the 'method' argument of pandas.DataFrame.fillna
    sampling_methods = {None, 'ffill', 'pad', 'backfill', 'bfill'}

    __slots__ = ()

    def __new__(cls, name, dims, upsample, downsample):
        # sanity check dims
        if int(dims) < 0:
            raise ValueError("Dims must be positive not {}".format(dims))

        # sanity check sample method
        for sample in (upsample, downsample):
            if not (sample in cls.sampling_methods or callable(sample)):
                raise ValueError("{} is not a valid sampling method. It "
                                 "must be one of {} or callable".format(
                                     sample, cls.sampling_methods))

        # pass everything up to base class
        return super(ColSpec, cls).__new__(
            cls, name, dims, upsample, downsample)


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
    col_info : list
        List of information about the columns. Each entry should
        be a tuple of the form (col_name, fill_method, dimensionality). See
        `ColSpec` class docstring

    max_frames : int, optional
        The maximum number of frames for the non-scalar columns

    use_pims_fs : bool, optional
        If the DM should use a pims frame-store to deal with 2D data.
        It allows file names to be added transparently, but max_frames
        won't apply to frames added as arrays.

    """
    def __init__(self, events=None):
        self.sources = {}
        self.specs = {}
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
                col_spec = ColSpec(name, dim, 'ffill', np.mean)  # TEMP!
                self.specs[name] = col_spec

            # TODO Handle nonscalar data
            self._data.append(event.data)
            self._time.append(event.time)

    @property
    def _dataframe(self):
        # Rebuild the DataFrame if more data has been added.
        if self._stale:
            self._df = pd.DataFrame(list(self._data), index=list(self._time))
            self._stale = False
        return self._df

    def bin_by_edges(self, bin_edges):
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
        binning = np.zeros(len(self._time), dtype=np.bool)
        for i, pair in enumerate(bin_edges):
            binning[(self._time < pair[0]) & (self._time > pair[1])] = i
        return self.resample(binning)

    def resample(self, binning, agg=None, interpolate=None):
        grouped = self._dataframe.groupby(binning)

        # 1. How many (non-null) data points in each bin?
        counts = grouped.count()

        # 2. If upsampling is possible, put one point in the center of
        #    each bin.
        upsampled_df = pd.DataFrame()
        for col_name in upsampled_df:
            upsampling_rule = self.upsampling_rules[col_name]
            col_data = self._dataframe[col_name]
            if upsampling_rule is None:
                # Copy the column with no changes.
                upsampled_df[col_name] = col_data
            elif upsampling_rule in ColSpec.upsampling_methods:
                # Then method must be a string.
                upsampled_df[col_name] = col_data.fillna(upsampling_rule)
            else:
                # The method is a callable. For sample, a curried
                # pandas.rolling_apply would make sense here.
                upsampled_df[col_name] = upsampling_rule(col_data)

    def add_column(self, col_info):
        """
        Adds a column to the DataMuggler

        Parameters
        ----------
        col_info : tuple
            Of the form (col_name, fill_method, dimensionality). See
           `ColSpec` class docstring
        """
        # make sure we got valid input
        col_info = ColSpec(*col_info)

        # check that the column with the same name does not exist
        if col_info.name in [c.name for c in self._col_info]:
            raise ValueError(
                "The key {} already exists in the DM".format(col_info.name))

        # stash the info for future lookup
        self._col_info.append(col_info)
        # stash the fill method
        self._col_fill[col_info.name] = col_info.fill_method
        # check if we need to deal with none-scalar data
        if col_info.dims > 0:
            self._is_col_nonscalar.add(col_info.name)
            if self._use_fs and col_info.dims == 2:
                self._framestore[col_info.name] = ImageSeq(None)
            else:
                self._nonscalar_col_lookup[col_info.name] = OrderedDict()

        self._dataframe[col_info.name] = pd.Series(np.nan,
                                                   index=self._dataframe.index)

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
