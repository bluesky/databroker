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
from collections import namedtuple, OrderedDict, deque, Iterable
import warnings
import logging
import pandas as pd
import numpy as np
from scipy.interpolate import interp1d, interp2d


logger = logging.getLogger(__name__)
__all__ = ['DataMuggler', 'dataframe_to_dict']


class BinningError(Exception):
    """
    An exception to raise if there are insufficient sampling rules to
    upsampling or downsample a data column into specified bins.
    """
    pass

class BadDownsamplerError(Exception):
    """
    An exception to raise if a downsampler produces unexpected output.
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
    downsample : None or a function
        None if the data cannot be downsampled (reduced). Otherwise,
        any callable that reduces multiple data points (of whatever dimension)
        to a single data point.

    References
    ----------
    http://docs.scipy.org/doc/scipy/reference/generated/scipy.interpolate.interp1d.html
    """
    # These reflect the 'method' argument of pandas.DataFrame.fillna
    upsampling_methods = {None, 'linear', 'nearest', 'zero', 'slinear', 
                        'quadratic', 'cubic'}

    __slots__ = ()

    def __new__(cls, name, ndim, upsample, downsample):
        # Validations
        upsample = _validate_upsample(upsample)
        downsample = _validate_downsample(downsample)
        if int(ndim) < 0:
            raise ValueError("ndim must be positive not {}".format(ndim))

        return super(ColSpec, cls).__new__(
            cls, name, ndim, upsample, downsample)


def _validate_upsample(input):
    # TODO The upsampling method could be any callable.
    if input is None:
        return input
    if not (input in ColSpec.upsampling_methods):
        raise ValueError("{} is not a valid upsampling method. It "
                         "must be one of {}".format(
                         input, cls.upsampling_methods))
    return input.lower()


def _validate_downsample(input):
    # TODO The downsampling methods could have string aliases like 'mean'.
    if (input is not None) and (not callable(input)):
        raise ValueError("The downsampling method must be a callable "
                         "or None.")
    return input


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
        self._col_info = {}
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
                    ndim = 0

                col_info = ColSpec(name, ndim, None, None)  # defaults
                # TODO Look up source-specific default in a config file
                # or some other source of reference data.
                self._col_info[name] = col_info

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
            self._df.index.name = 'epoch_time'
            self._stale = False
        return self._df

    def bin_on(self, source_name, interpolation=None, agg=None):
        """
        Return data resampled to align with the data from a particular source.

        Parameters
        ----------
        source_name : string
        interpolation : dict
            Override the default interpolation (upsampling) behavior of any
            data source by passing a dictionary of source names mapped onto
            one of the following interpolation methods.

            {None, 'linear', 'nearest', 'zero', 'slinear', 'quadratic',
             'cubic'}

            None means that each time bin must have at least one value.
            See scipy.interpolator for more on the other methods.
        agg : dict
            Override the default reduction (downsampling) behavior of any data
            source by passing a dictionary of source names mapped onto any
            callable that reduces multiple data points (of whatever dimension)
            to a single data point.

        Returns
        -------
        resampled_df : pandas.DataFrame

        References
        ----------
        http://docs.scipy.org/doc/scipy/reference/generated/scipy.interpolate.interp1d.html
        """
        time = np.array(self._time)
        col = self._dataframe.sort()[source_name]
        centers = col.dropna().index.values

        # [2, 4, 6] -> [-inf, 3, 5, inf]
        bin_edges = np.mean([centers[1:], centers[:-1]], 0)
        # [-inf, 3, 5, inf] -> [(-inf, 3), (3, 5), (5, inf)]
        bin_edges = [-np.inf] + list(np.repeat(bin_edges, 2)) + [np.inf]
        bin_edges = np.reshape(bin_edges, (-1, 2))
        return self.bin_by_edges(bin_edges, time_labels=centers,
                                 interpolation=interpolation, agg=agg)

    def bin_by_edges(self, bin_edges, anchor=None, time_labels=None,
                     interpolation=None, agg=None):
        """
        Return data resampled into bins with the specified edges.

        Parameters
        ----------
        bin_edges : list
            list of two-element items like [(t1, t2), (t3, t4), ...]
        anchor : {'left', 'center', 'right'}, optional
            By default, bins are labeled by their centers, but they can
            alternatively be labled by their left or right edge.
        time_labels : ndarray, optional
            Time points used to label each bin. Overrides anchor above.
        interpolation : dict
            Override the default interpolation (upsampling) behavior of any
            data source by passing a dictionary of source names mapped onto
            one of the following interpolation methods.

            {None, 'linear', 'nearest', 'zero', 'slinear', 'quadratic', 'cubic'}

            None means that each time bin must have at least one value.
            See scipy.interpolator for more on the other methods.
        agg : dict
            Override the default reduction (downsampling) behavior of any data
            source by passing a dictionary of source names mapped onto any
            callable that reduces multiple data points (of whatever dimension)
            to a single data point.

        Returns
        -------
        resampled_df : pandas.DataFrame

        References
        ----------
        http://docs.scipy.org/doc/scipy/reference/generated/scipy.interpolate.interp1d.html
        """
        time = np.array(self._time)
        # Get edges into 1D array[L, R, L, R, ...]
        edges_as_pairs = np.reshape(bin_edges, (-1, 2))
        all_edges = np.ravel(edges_as_pairs)
        if not np.all(np.diff(all_edges) >= 0):
            raise ValueError("Illegal binning: the left edge must be less "
                             "than the right edge.")
        # Sort out where the array each time would be inserted.
        binning = np.searchsorted(all_edges, time).astype(float)
        # Times that would get inserted at even positions are between bins.
        # Mark them 
        binning[binning % 2 == 0] = np.nan
        binning //= 2  # Make bin number sequential, not odds only.
        bin_count = pd.Series(binning).nunique()  # not including NaN
        if anchor is None and time_labels is None:
            anchor = 'center'
        if time_labels is not None:
            if len(time_labels) != bin_count:
                raise ValueError("The number of time_labels ({0}) must equal "
                                 "the number of bins ({1}).".format(
                                 len(time_labels), bin_count))
        elif isinstance(anchor, six.string_types):
            if anchor == 'left':
                time_labels = edges_as_pairs[:, 0]
            elif anchor == 'center':
                time_labels = np.mean(edges_as_pairs, axis=1)
            elif anchor == 'right':
                time_labels = edges_as_pairs[:, 1]
            else:
                raise ValueError("anchor must be 'left', 'center', 'right', "
                                 "or None") 
        return self.resample(time_labels, binning, interpolation, agg)

    def resample(self, time_labels, binning, interpolation=None, agg=None,
                 verify_integrity=True):
        resampled_df = pd.DataFrame(index=np.arange(len(time_labels)))

        # How many (non-null) data points in each bin?
        grouped = self._dataframe.groupby(binning)
        counts = grouped.count()
        has_one_point = counts == 1
        has_no_points = counts == 0
        has_multiple_points = ~(has_one_point | has_no_points)
        # Get the first (maybe only) point in each bin.
        first_point = grouped.first()

        for name in self._dataframe:
            # Resolve (and if necessary validate) sampling rules.
            col_info = self._col_info[name]
            try:
                upsample = interpolation[name]
            except (TypeError, KeyError):
                upsample = col_info.upsample
            else:
                upsample = _validate_upsample(upsample)
            try:
                downsample = agg[name]
            except (TypeError, KeyError):
                downsample = col_info.downsample
            else:
                downsample = _validate_downsample(downsample)

            # Start by using the first point in a bin. (If there are actually
            # multiple points, we will either overwrite or raise below.)
            resampled_df[name] = first_point[name]

            # Short-circuit if we are done.
            if np.all(has_one_point[name]):
                continue

            # If any bin has no data, use the upsampling rule to interpolate
            # at the center of the empty bins. If there is no rule, simply
            # leave some bins empty. Do not raise an error.
            if upsample is not None:
                dense_col = self._dataframe[name].dropna()
                x, y = dense_col.index.values, dense_col.values
                interpolator = interp1d(x, y, kind=upsample)
                # Outside the limits of the data, the interpolator will fail.
                # Leave any such entires empty.
                is_safe = (time_labels > np.min(x)) & (time_labels < np.max(x))
                safe_times = time_labels[is_safe]
                safe_bins = np.arange(len(time_labels))[is_safe]
                interpolated_points = pd.Series(interpolator(safe_times),
                                                index=safe_bins)
                logger.debug("Interpolating to fill %d of %d empty bins in %s",
                             len(safe_bins), has_no_points[name].sum(), name)
                resampled_df[name].fillna(interpolated_points, inplace=True)

            # Short-circuit if we are done.
            if np.all(~has_multiple_points[name]):
                continue

            # Multi-valued bins must be downsampled (reduced). If there is no
            # rule for downsampling, we have no recourse: we must raise.
            if downsample is None:
                raise BinningError("The specified binning puts multiple "
                                   "'{0}' measurements in at least one bin, "
                                   "and there is no rule for downsampling "
                                   "(i.e., reducing) it.".format(name))
            if verify_integrity:
                expected_shape = 0  # TODO get real shape from descriptor
                downsample = _build_safe_downsample(downsample, expected_shape)
            downsampled = grouped[name].agg({name: downsample}).squeeze()
            resampled_df[name].where(~has_multiple_points[name], downsampled,
                                     inplace=True)

        # Label the bins with time points.
        resampled_df.index = time_labels
        return resampled_df

    def __getitem__(self, source_name):
        if source_name not in self._col_info.keys():
            raise KeyError("No data from a source called '{0}' has been "
                           "added.".format(source_name))
        # TODO Dispatch a query to the broker?
        return self._dataframe[source_name].dropna()

    def __getattr__(self, attr):
        if attr in self._col_info.keys():
            return self[attr]
        else:
            raise AttributeError("DataMuggler has no attribute {0} and no "
                "data source named '{0}'".format(attr))

    @property
    def col_ndim(self):
        """
        The dimensionality of the data stored in all columns. Returned as a
        dictionary keyed on column name.

         0 -> scalar
         1 -> line (MCA spectra)
         2 -> image
         3 -> volume
        """
        return {c.name: c.ndim for c in self._col_info}

    @property
    def ncols(self):
        """
        The number of columns that the DataMuggler contains
        """
        return len(self._col_info)

    @property
    def col_downsample_rules(self):
        """
        Downsampling (reduction) rules for all of the columns.
        """
        return {c.name: c.downsample for c in self._col_info}

    @property
    def col_upsample_rules(self):
        """
        Upsampling (interpolation) rules for all of the columns.
        """
        return {c.name: c.upsample for c in self._col_info}


def dataframe_to_dict(df):
    """
    Turn a DataFrame into a dict of lists.

    Parameters
    ----------
    df : DataFrame

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


def _build_safe_downsample(downsample, expected_shape):
    # Ensure two things:
    # 1. The downsampling function shouldn't touch bins with only one point.
    # 2. The result of downsample should have the right shape.
    def _downsample(data):
        if len(data) == 1:
            return data
        downsampled = downsample(data)
        if (expected_shape is None or expected_shape == 0):
            if not np.isscalar(downsampled):
                raise BadDownsamplerError("The 'agg' (downsampling) function "
                                          "for {0} is expected to produce "
                                          "a scalar from the data in each "
                                          "bin.".format(downsampled))
        elif downsampled.shape != expected_shape:
            raise BadDownsamplerError("The 'agg' (downsampling) function for "
                                      "{0} returns data shaped {1} but the "
                                      "shape {2} is expected.".format(
                                      name, downsampled.shape, expected_shape))
        return downsampled
    return _downsample
