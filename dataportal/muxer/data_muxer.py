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
from collections import namedtuple, deque
import logging
import pandas as pd
import tzlocal
import numpy as np
from scipy.interpolate import interp1d
import pandas.core.groupby  # to get custom exception


logger = logging.getLogger(__name__)
__all__ = ['DataMuxer', 'dataframe_to_dict']

TZ = str(tzlocal.get_localzone())


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
              'ColSpec', ['name', 'ndim', 'shape', 'upsample', 'downsample'])):
    """
    Named-tuple sub-class to validate the column specifications for the
    DataMuxer

    Parameters
    ----------
    name : hashable
    ndim : uint
        Dimensionality of the data stored in the column
    shape : tuple or None
        like ndarray.shape, where 0 or None are scalar
    upsample : {None, 'linear', 'nearest', 'zero', 'slinear', 'quadratic', 'cubic', 'ffill', 'bfill'}
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
    upsampling_methods = {'None', 'linear', 'nearest', 'zero', 'slinear',
                          'quadratic', 'cubic', 'ffill', 'bfill'}
    downsampling_methods = {'None', 'last', 'first', 'median', 'mean', 'sum',
                            'min', 'max'}
    _downsample_mapping = {'last': lambda x: x[-1],
                           'first': lambda x: x[0],
                           # new in np 1.9
                           'median': lambda x: np.median(x, 0),
                           'mean': lambda x: np.mean(x, 0),
                           'sum': lambda x: np.sum(x, 0),
                           'min': lambda x: np.min(x, 0),
                           'max': lambda x: np.max(x, 0)}

    __slots__ = ()

    def __new__(cls, name, ndim, shape, upsample, downsample):
        # Validations
        upsample = _validate_upsample(upsample)
        downsample = _validate_downsample(downsample)
        if int(ndim) < 0:
            raise ValueError("ndim must be positive not {}".format(ndim))
        if shape is not None:
            shape = tuple(shape)

        return super(ColSpec, cls).__new__(
            cls, name, int(ndim), shape, upsample, downsample)


def _validate_upsample(input):
    # TODO The upsampling method could be any callable.
    if input is None or input == 'None':
        return 'None'
    if not (input in ColSpec.upsampling_methods):
        raise ValueError("{} is not a valid upsampling method. It "
                         "must be one of {}".format(
                             input, ColSpec.upsampling_methods))
    return input.lower()


def _validate_downsample(input):
    # TODO The downsampling methods could have string aliases like 'mean'.
    if (input is not None) and (not (callable(input) or
                                     input in ColSpec.downsampling_methods)):
        raise ValueError("The downsampling method must be a callable, None, "
                         "or one of {}.".format(ColSpec.downsampling_methods))
    if input is None:
        return 'None'
    return input


class DataMuxer(object):
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
    class Planner(object):
        def __init__(self, dm):
            self.dm = dm

        def determine_upsample(self, interpolation=None, use_cols=None):
            "Resolve (and if necessary validate) upsampling rules."
            if interpolation is None:
                interpolation = dict()
            if use_cols is None:
                use_cols = self.dm.columns
            rules = dict()
            for name in use_cols:
                col_info = self.dm.col_info[name]
                rule = _validate_upsample(
                    interpolation.get(name, col_info.upsample))
                rule = _normalize_string_none(rule)
                if (rule is not None) and (col_info.ndim > 0):
                    raise NotImplementedError(
                        "Only scalar data can be upsampled. "
                        "The {0}-dimensional source {1} was given the "
                        "upsampling rule {2}.".format(
                            col_info.ndim, name, rule))
                rules[name] = rule
            return rules

        def determine_downsample(self, agg=None, use_cols=None):
            "Resolve (and if necessary validate) sampling rules."
            if agg is None:
                agg = dict()
            if use_cols is None:
                use_cols = self.dm.columns
            rules = dict()
            for name in use_cols:
                col_info = self.dm.col_info[name]
                rule = _validate_downsample(agg.get(name, col_info.downsample))
                rule = _normalize_string_none(rule)
                rules[name] = rule
            return rules

        def bin_by_edges(self, bin_edges, bin_anchors, interpolation=None,
                         agg=None, use_cols=None):
            """Explain operation of DataMuxer.bin_by_edges

            Parameters
            ----------
            bin_edges : list
                list of two-element items like [(t1, t2), (t3, t4), ...]
            bin_anchors : list
                These are time points where interpolated values will be
                evaluated. Bin centers are usually a good choice.
            interpolation : dict, optional
                Override the default interpolation (upsampling) behavior of any
                data source by passing a dictionary of source names mapped onto
                one of the following interpolation methods.

                {None, 'linear', 'nearest', 'zero', 'slinear', 'quadratic',
                'cubic', 'ffill', 'bfill'}

                None means that each time bin must have at least one value.
                See scipy.interpolator for more on the other methods.
            agg : dict, optional
                Override the default reduction (downsampling) behavior of any
                data source by passing a dictionary of source names mapped onto
                any callable that reduces multiple data points (of whatever
                dimension) to a single data point.
            use_cols : list, optional
                List of columns to include in binning; use all columns by
                default.

            Returns
            -------
            df : pandas.DataFrame
                table giving upsample and downsample rules for each data column
                and indicating whether those rules are applicable

            References
            ----------
            http://docs.scipy.org/doc/scipy/reference/generated/scipy.interpolate.interp1d.html
            """
            bin_anchors, binning = self.dm._bin_by_edges(bin_anchors, bin_edges)
            # TODO Cache the grouping for reuse by resample.
            grouped = self.dm._dataframe.groupby(binning)
            counts = grouped.count()
            df = pd.DataFrame.from_dict(_is_resampling_applicable(counts))
            df['upsample'] = self.determine_upsample(interpolation, use_cols)
            df['downsample'] = self.determine_downsample(agg, use_cols)
            return df

        def bin_on(self, source_name, interpolation=None, agg=None,
                   use_cols=None):
            """Explain operation of DataMuxer.bin_on.

            Parameters
            ----------
            source_name : string
            interpolation : dict, optional
                Override the default interpolation (upsampling) behavior of any
                data source by passing a dictionary of source names mapped onto
                one of the following interpolation methods.

                {None, 'linear', 'nearest', 'zero', 'slinear', 'quadratic',
                'cubic'}

                None means that each time bin must have at least one value.
                See scipy.interpolator for more on the other methods.
            agg : dict, optional
                Override the default reduction (downsampling) behavior of any
                data source by passing a dictionary of source names mapped onto
                any callable that reduces multiple data points (of whatever
                dimension) to a single data point.
            use_cols : list, optional
                List of columns to include in binning; use all columns by
                default.

            Returns
            -------
            df : pandas.DataFrame
                table giving upsample and downsample rules for each data column
                and indicating whether those rules are applicable

            References
            ----------
            http://docs.scipy.org/doc/scipy/reference/generated/scipy.interpolate.interp1d.html
            """
            centers, bin_edges = self.dm._bin_on(source_name)
            bin_anchors, binning = self.dm._bin_by_edges(centers, bin_edges)
            # TODO Cache the grouping for reuse by resample.
            grouped = self.dm._dataframe.groupby(binning)
            counts = grouped.count()
            df = pd.DataFrame.from_dict(_is_resampling_applicable(counts))
            df['upsample'] = self.determine_upsample(interpolation, use_cols)
            df['downsample'] = self.determine_downsample(agg, use_cols)
            return df

    default_upsample = None
    default_downsample = None

    def __init__(self):
        self.sources = {}
        self.col_info = {}
        self.col_info['time'] = ColSpec('time', 0, [], 'linear', 'mean')

        self._data = deque()
        self._time = deque()
        self._timestamps = deque()

        self._timestamps_as_data = set()
        self._known_events = set()
        self._known_descriptors = set()
        self._stale = True

        self.plan = self.Planner(self)
        self.convert_times = True
        self._reference_time = None

    @property
    def reference_time(self):
        return self._reference_time

    @reference_time.setter
    def reference_time(self, val):
        self._reference_time = pd.Timestamp(val, unit='s')

    @property
    def columns(self):
        "The columns of DataFrames returned by methods that return DataFrames."
        return set(self.sources) | self._time_columns

    @property
    def _time_columns(self):
        ts_names = [name + '_timestamp' for name in self._timestamps_as_data]
        return {'time'} | set(ts_names)

    @classmethod
    def from_events(cls, events, verbose=False):
        """
        Create a DataMuxer from a list of Events.

        Parameters
        ----------
        events : list
            list of Events (any objects with the expected attributes will do)
        """
        
        instance = cls()
        instance.append_events(events, verbose)
        return instance

    def append_events(self, events, verbose=False):
        """Add a list of events to the DataMuxer.

        Parameters
        ----------
        events : list
            list of Events (any objects with the expected attributes will do)
        """
        for idx, event in enumerate(events):
            if verbose and idx % 25 == 0:
                print('loading event %s' % idx),
            self.append_event(event)

    def append_event(self, event):
        """Add an event to the DataMuxer.

        Parameters
        ----------
        event : Event
            Event Document or any object with the expected attributes

        Returns
        -------
        is_new : bool
            True if event was added, False is it has already been added
        """
        if event.uid in self._known_events:
            return False
        self._known_events.add(event.uid)
        self._stale = True
        if event.descriptor.uid not in self._known_descriptors:
            self._process_new_descriptor(event.descriptor)
        # Both scalar and nonscalar data will get stored in the DataFrame.
        # This may be optimized later, but it might not actually help much.
        self._data.append(
            {name: data for name, data in six.iteritems(event.data)})
        self._timestamps.append(
            {name: ts for name, ts in six.iteritems(event.timestamps)})
        self._time.append(event.time)
        return True

    def _process_new_descriptor(self, descriptor):
        "Build a ColSpec and update state."
        for name, description in six.iteritems(descriptor.data_keys):

            # If we already have this source name, the unique source
            # identifiers must match. Ambiguous names are not allowed.
            if name in self.sources:
                if self.sources[name] != description['source']:
                    raise ValueError("In a previously loaded descriptor, "
                                     "'{0}' refers to {1} but in Event "
                                     "Descriptor {2} it refers to {3}.".format(
                                         name, self.sources[name],
                                         descriptor.uid,
                                         description['source']))
                if name == 'time':
                    # We can argue later about how best to handle this corner
                    # case, but anything is better than silently mislabeling
                    # data.
                    raise ValueError("The name 'time' is reserved and cannot "
                                     "be used as an alias.")

            # If it is a new name, determine a ColSpec.
            else:
                self.sources[name] = description['source']
                if 'external' in description and 'shape' in description:
                    shape = description['shape']
                    ndim = len(shape)
                else:
                    # External data can be scalar. Nonscalar data must
                    # have a specified shape. Thus, if no shape is given,
                    # assume scalar.
                    shape = None
                    ndim = 0
                upsample = self.default_upsample
                if ndim > 0:
                    upsample = None

                col_info = ColSpec(name, ndim, shape, upsample,
                                   self.default_downsample)  # defaults
                # TODO Look up source-specific default in a config file
                # or some other source of reference data.
                self.col_info[name] = col_info
        self._known_descriptors.add(descriptor.uid)

    @property
    def _dataframe(self):
        "See also to_sparse_dataframe, the public version of this."
        # Rebuild the DataFrame if more data has been added.
        if self._stale:
            df = pd.DataFrame(list(self._data))
            df['time'] = list(self._time)
            if self._timestamps_as_data:
                # Only build this if we need it.
                # TODO: We shouldn't have to build
                # the whole thing, but there is already a lot of trickiness
                # here so we'll worry about optimization later.
                timestamps = pd.DataFrame(list(self._timestamps))
            for source_name in self._timestamps_as_data:
                col_name = _timestamp_col_name(source_name)
                df[col_name] = timestamps[source_name]
                logger.debug("Including %s timestamps as data", source_name)
            self._df = df.sort('time').reset_index(drop=True)
            self._stale = False
        return self._df

    def to_sparse_dataframe(self, include_all_timestamps=False):
        """Obtain all measurements in a DataFrame, one row per Event time.

        Parameters
        ----------
        include_all_timestamps : bool
            The result will always contain a 'time' column but, by default,
            not timestamps for individual data sources like 'motor_timestamp'.
            Set this to True to export timestamp columns for each data column

        Returns
        -------
        df : pandas.DataFrame
        """
        if include_all_timestamps:
            raise NotImplementedError("TODO")

        result = self._dataframe.copy()
        for col_name in self._time_columns:
            result[col_name] = self._maybe_convert_times(result[col_name])
        return result

    def _maybe_convert_times(self, data):
        if self.convert_times:
            t = pd.to_datetime(data, unit='s', utc=True).dt.tz_localize(TZ)
            if self.reference_time is None:
                return t
            else:
                return t - self.reference_time
        return data  # no-op

    def include_timestamp_data(self, source_name):
        """Add the exact timing of a data source as a data column.

        Parameters
        ----------
        source_name : string
            one of the source names in DataMuxer.sources
        """
        # self._timestamps_as_data is a set of sources who timestamps
        # should be treated as data in the _dataframe method above.
        self._timestamps_as_data.add(source_name)
        name = _timestamp_col_name(source_name)
        self.col_info[name] = ColSpec(name, 0, None, None, np.mean)
        self._stale = True

    def remove_timestamp_data(self, source_name):
        """Remove the exact timing of a data source from the data columns.

        Parameters
        ----------
        source_name : string
            one of the source names in DataMuxer.sources
        """
        self._timestamps_as_data.remove(source_name)
        # Do not force a rebuilt (i.e., self._stale). Just remove it here.
        del self._df[_timestamp_col_name(source_name)]

    def bin_on(self, source_name, interpolation=None, agg=None, use_cols=None):
        """
        Return data resampled to align with the data from a particular source.

        Parameters
        ----------
        source_name : string
        interpolation : dict, optional
            Override the default interpolation (upsampling) behavior of any
            data source by passing a dictionary of source names mapped onto
            one of the following interpolation methods.

            {None, 'linear', 'nearest', 'zero', 'slinear', 'quadratic',
             'cubic'}

            None means that each time bin must have at least one value.
            See scipy.interpolator for more on the other methods.
        agg : dict, optional
            Override the default reduction (downsampling) behavior of any data
            source by passing a dictionary of source names mapped onto any
            callable that reduces multiple data points (of whatever dimension)
            to a single data point.
        use_cols : list, optional
            List of columns to include in binning; use all columns by default.

        Returns
        -------
        resampled_df : pandas.DataFrame

        References
        ----------
        http://docs.scipy.org/doc/scipy/reference/generated/scipy.interpolate.interp1d.html
        """
        centers, bin_edges = self._bin_on(source_name)
        return self.bin_by_edges(bin_edges, bin_anchors=centers,
                                 interpolation=interpolation, agg=agg,
                                 use_cols=use_cols)

    def _bin_on(self, source_name):
        "Compute bin edges spaced around centers defined by source_name points."
        col = self._dataframe[source_name]
        centers = self._dataframe['time'].reindex_like(col.dropna()).values

        # [2, 4, 6] -> [-inf, 3, 5, inf]
        bin_edges = np.mean([centers[1:], centers[:-1]], 0)
        # [-inf, 3, 5, inf] -> [(-inf, 3), (3, 5), (5, inf)]
        bin_edges = [-np.inf] + list(np.repeat(bin_edges, 2)) + [np.inf]
        bin_edges = np.reshape(bin_edges, (-1, 2))
        return centers, bin_edges

    def bin_by_edges(self, bin_edges, bin_anchors, interpolation=None, agg=None,
                     use_cols=None):
        """
        Return data resampled into bins with the specified edges.

        Parameters
        ----------
        bin_edges : list
            list of two-element items like [(t1, t2), (t3, t4), ...]
        bin_anchors : list
            These are time points where interpolated values will be evaluated.
            Bin centers are usually a good choice.
        interpolation : dict, optional
            Override the default interpolation (upsampling) behavior of any
            data source by passing a dictionary of source names mapped onto
            one of the following interpolation methods.

            {None, 'linear', 'nearest', 'zero', 'slinear', 'quadratic',
             'cubic'}

            None means that each time bin must have at least one value.
            See scipy.interpolator for more on the other methods.
        agg : dict, optional
            Override the default reduction (downsampling) behavior of any data
            source by passing a dictionary of source names mapped onto any
            callable that reduces multiple data points (of whatever dimension)
            to a single data point.
        use_cols : list, optional
            List of columns to include in binning; use all columns by default.

        Returns
        -------
        resampled_df : pandas.DataFrame

        References
        ----------
        http://docs.scipy.org/doc/scipy/reference/generated/scipy.interpolate.interp1d.html
        """
        bin_anchors, binning = self._bin_by_edges(bin_anchors, bin_edges)
        return self.resample(bin_anchors, binning, interpolation, agg,
                             use_cols=use_cols)

    def _bin_by_edges(self, bin_anchors, bin_edges):
        "Compute bin assignment and, if needed, bin_anchors."
        time = self._dataframe['time'].values
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
        if bin_anchors is None:
            bin_anchors = np.mean(edges_as_pairs, axis=1)  # bin centers
        else:
            if len(bin_anchors) != len(bin_edges):
                raise ValueError("There are {0} bin_anchors but {1} pairs of "
                                 "bin_edges. These must match.".format(
                                     len(bin_anchors), len(bin_edges)))
        return bin_anchors, binning

    def resample(self, bin_anchors, binning, interpolation=None, agg=None,
                 verify_integrity=True, use_cols=None):
        """
        Return data resampled into bins with the specified edges.

        Parameters
        ----------
        bin_anchors : list
            These are time points where interpolated values will be evaluated.
            Bin centers are usually a good choice.
        bin_anchors : list
            Bin assignment. Example: [1, 1, 2, 2, 3, 3] puts six data points
            into three bins with two points each.
        interpolation : dict, optional
            Override the default interpolation (upsampling) behavior of any
            data source by passing a dictionary of source names mapped onto
            one of the following interpolation methods.

            {None, 'linear', 'nearest', 'zero', 'slinear', 'quadratic',
             'cubic'}

            None means that each time bin must have at least one value.
            See scipy.interpolator for more on the other methods.
        agg : dict, optional
            Override the default reduction (downsampling) behavior of any data
            source by passing a dictionary of source names mapped onto any
            callable that reduces multiple data points (of whatever dimension)
            to a single data point.
        verify_integrity : bool, optional
            For a cost in performance, verify that the downsampling function
            produces data of the expected shape. True by default.
        use_cols : list, optional
            List of columns to include in binning; use all columns by default.

        Returns
        -------
        resampled_df : pandas.DataFrame

        References
        ----------
        http://docs.scipy.org/doc/scipy/reference/generated/scipy.interpolate.interp1d.html
        """
        if use_cols is None:
            use_cols = self.columns
        plan = self.Planner(self)
        upsampling_rules = plan.determine_upsample(interpolation, use_cols)
        downsampling_rules = plan.determine_downsample(agg, use_cols)
        grouped = self._dataframe.groupby(binning)
        first_point = grouped.first()
        counts = grouped.count()
        resampling_requirements = _is_resampling_applicable(counts)
        index = np.arange(len(bin_anchors))
        result = {}  # dict of DataFrames, to become one MultiIndexed DataFrame
        for name in use_cols:
            upsample = upsampling_rules[name]
            downsample = downsampling_rules[name]
            upsampling_possible = resampling_requirements['upsampling_possible'][name]
            downsampling_needed = resampling_requirements['downsampling_needed'][name]
            result[name] = pd.DataFrame(index=index)
            # Put the first (maybe only) value into a Series.
            # We will overwrite as needed below.
            result[name]['val'] = pd.Series(data=first_point[name])

            # Short-circuit if we are done.
            if not (upsampling_possible or downsampling_needed):
                logger.debug("%s has exactly one data point per bin", name)
                continue

            result[name]['count'] = counts[name]

            # If any bin has no data, use the upsampling rule to interpolate
            # at the center of the empty bins. If there is no rule, simply
            # leave some bins empty. Do not raise an error.
            if upsampling_possible and (upsample is not None):
                if upsample in ('ffill', 'bfill'):
                    result[name]['val'].fillna(method=upsample, inplace=True)
                else:
                    dense_col = self._dataframe[name].dropna()
                    y = dense_col.values
                    x = self._dataframe['time'].reindex_like(dense_col).values
                    interpolator = interp1d(x, y, kind=upsample)
                    # Outside the limits of the data, the interpolator will
                    # fail.  Leave any such entires empty.
                    is_safe = ((bin_anchors > np.min(x)) &
                               (bin_anchors < np.max(x)))
                    safe_times = bin_anchors[is_safe]
                    safe_bins = index[is_safe]
                    interp_points = pd.Series(interpolator(safe_times),
                                              index=safe_bins)
                    logger.debug("Interpolating to fill %d of %d "
                                 "empty bins in %s",
                                 len(safe_bins), (counts[name] == 0).sum(),
                                 name)
                    result[name]['val'].fillna(interp_points, inplace=True)

            # Short-circuit if we are done.
            if not downsampling_needed:
                logger.debug("%s has at most one data point per bin", name)
                continue

            # Multi-valued bins must be downsampled (reduced). If there is no
            # rule for downsampling, we have no recourse: we must raise.
            if (downsample is None):
                raise BinningError("The specified binning puts multiple "
                                   "'{0}' measurements in at least one bin, "
                                   "and there is no rule for downsampling "
                                   "(i.e., reducing) it.".format(name))
            if verify_integrity and callable(downsample):
                downsample = _build_verified_downsample(
                    downsample, self.col_info[name].shape)

            g = grouped[name]  # for brevity
            if self.col_info[name].ndim == 0:
                logger.debug("The scalar column %s must be downsampled.", name)
                # For scalars, pandas knows what to do.
                downsampled = g.agg(downsample)
                std_series = g.std()
                max_series = g.max()
                min_series = g.min()
            else:
                # For nonscalars, we are abusing groupby and must go to a
                # a little more trouble to guarantee success.
                logger.debug("The nonscalar column %s must be downsampled.",
                             name)
                if not callable(downsample):
                    # Do this lookup here so that strings can be passed
                    # in the call to resample.
                    downsample = ColSpec._downsample_mapping[downsample]
                downsampled = g.apply(lambda x: downsample(np.asarray(x.dropna())))
                std_series = g.apply(lambda x: np.std(np.asarray(x.dropna()), 0))
                max_series = g.apply(lambda x: np.max(np.asarray(x.dropna()), 0))
                min_series = g.apply(lambda x: np.min(np.asarray(x.dropna()), 0))

            # This (counts[name] > 1) is redundant, but there is no clean way to
            # pass it here without refactoring. Not a huge cost.
            result[name]['val'].where(~(counts[name] > 1), downsampled, inplace=True)
            result[name]['std'] = std_series
            result[name]['max'] = max_series
            result[name]['min'] = min_series

        result = pd.concat(result, axis=1)  # one MultiIndexed DataFrame
        result.index.name = 'bin'

        # Convert time timestamp or timedelta, depending on the state of
        # self.convert_times and self.reference_time.
        for col_name in self._time_columns:
            if isinstance(result[col_name], pd.DataFrame):
                subcols = result[col_name].columns
                for subcol in subcols & {'max', 'min', 'val'}:
                    result[(col_name, subcol)] = self._maybe_convert_times(
                            result[(col_name, subcol)])
                for subcol in subcols & {'std'}:
                    result[(col_name, subcol)] = pd.to_timedelta(
                            result[(col_name, subcol)], unit='s')
            else:
                result[col_name] = self._maybe_convert_times(
                        result[col_name])
        return result

    def __getitem__(self, source_name):
        if source_name not in list(self.col_info.keys()) + ['time']:
            raise KeyError("No data from a source called '{0}' has been "
                           "added.".format(source_name))
        # Unlike output from binning functions, this is indexed
        # on time.
        result = self._dataframe[source_name].dropna()
        result.index = self._dataframe['time'].reindex_like(result)
        return result

    def __getattr__(self, attr):
        # Developer beware: if any properties raise an AttributeError,
        # this will mask it. Comment this magic method to debug properties.
        if attr in self.col_info.keys():
            return self[attr]
        else:
            raise AttributeError("DataMuxer has no attribute {0} and no "
                                  "data source named '{0}'".format(attr))

    @property
    def ncols(self):
        """
        The number of columns that the DataMuxer contains
        """
        return len(self.col_info)

    @property
    def col_info_by_ndim(self):
        """Dictionary mapping dimensionality (ndim) onto a list of ColSpecs"""

        result = {}
        for name, col_spec in six.iteritems(self.col_info):
            try:
                result[col_spec.ndim]
            except KeyError:
                result[col_spec.ndim] = []
            result[col_spec.ndim].append(col_spec)
        return result


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
    dict_of_lists = {col: df[col].to_list() for col in df.columns}
    return df.index.values, dict_of_lists


def _build_verified_downsample(downsample, expected_shape):
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
            raise BadDownsamplerError("An 'agg' (downsampling) function "
                                      "returns data shaped {0} but the "
                                      "shape {1} is expected.".format(
                                          downsampled.shape, expected_shape))
        return downsampled
    return _downsample


def _timestamp_col_name(source_name):
    return '{0}_timestamp'.format(source_name)


def _normalize_string_none(val):
    "Replay passes 'None' to mean None."
    try:
        lowercase_val = val.lower()
    except AttributeError:
        return val
    if lowercase_val == 'none':
        return None
    else:
        return val


def _is_resampling_applicable(counts):
    has_no_points = counts == 0
    has_multiple_points = counts > 1
    upsampling_possible = has_no_points.any()
    downsampling_needed = has_multiple_points.any()
    result = {}
    result['upsampling_possible'] = upsampling_possible.to_dict()
    result['downsampling_needed'] = downsampling_needed.to_dict()
    return result
