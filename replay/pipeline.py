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
from . import QtCore

import pandas as pd


class PipelineComponent(QtCore.QObject):
    """
    The top-level object to represent a component in the quick-and-dirty
    live-data pipe line.

    This class provides the basic machinery for the signal and slot required for
    the hooking up the pipeline.

    This is meant to be sub-classed and sub classes must implement _process_msg
    which must have the signature::

        def _process_msg(self, message, data_object):
            return result_message, result_data

    This function can also return `None`

    The scheme for managing the messages will be pinned down at a later date.

    Currently this scheme does no checking to ensure types are correct or
    valid pre-running
    """
    source_signal = QtCore.Signal(str, object)

    @QtCore.Slot(str, object)
    def sink_slot(self, message, data_payload):
        """
        This function is the entry point for pushing data through
        this node in the pipeline.

        Parameters
        ----------
        message : str
            Some sort of string describing what the incoming data is

        data_payload : object
            The data to be processed.


        """
        try:
            ret = self._process_msg(message, data_payload)
        except Exception as E:
            # yes, catch them all!!
            print(E)
        else:
            if ret is not None:
                self.source_signal.emit(*ret)


class __muggler_helper(object):
    """
    This is a helper class for providing slicable objects
    out of the muggler.  This is mostly an api translation layer
    with very little brains, it delegates all useful work back
    up to it's parent DataMuggler.

    It might be worth (down the road) refactoring some of the PIMS
    base classes to provide slicing power
    """
    def __init__(self, muggler, column):
        self._muggler = muggler
        self._col = column

    def __getitem__(self, k):
        """
        Make this object slicable
        """
        return self._muggler.get_value(self._col, k)

    def __len__(self):
        return len(self._muggler)


class DataMuggler(QtCore.QObject):
    """
    This class provides a wrapper layer of signals and slots
    around a pandas DataFrame to make plugging stuff in for live
    view easier.

    The data collection/event model being used is all measurements
    (that is values that come off of the hardware) are time stamped
    to ring time.  The assumption that there will be one measurement
    (ex an area detector) which can not be interpolated and will serve
    as the source of reference time stamps.

    The language being used through out is that of pandas data frames.
    The data model is that of a sparse table keyed on time stamps which
    is 'densified' on demand by propagating the last measured value forward.



    Parameters
    ----------
    col_spec : list
        List of information about the columns. Each entry should
        be a tuple of the form (name, nafill_mode, is_scalar)

    """

    # this is a signal emitted when the muggler has new data that
    # clients can grab.  The list return is the names of the columns
    # that have new data
    new_data = QtCore.Signal(list)

    # make the muggler slicable so we can directly pass it to
    # 2D viewers
    def __init__(self, col_spec):
        valid_na_fill = {'pad', 'ffill', 'bfill', 'back pad'}

        self._fill_methods = dict()
        self._nonscalar_lookup = dict()
        self._is_not_scalar = set()
        names = []
        for c_name, fill_method, is_scalar in col_spec:
            # validate fill methods
            if fill_method not in valid_na_fill:
                raise ValueError(("{} is not a valid fill method "
                                 "must be one of {}").format(fill_method,
                                                             valid_na_fill))
            # used to sort out which way filling should be done.
            # forward for motor-like, backwards from image-like
            self._fill_methods[c_name] = fill_method
            # determine if the value should be stored directly in the data
            # frame or in a separate data structure
            if not is_scalar:
                self._is_not_scalar.add(c_name)
                self._nonscalar_lookup[c_name] = dict()
            names.append(c_name)

        # make an empty data frame
        self._dataframe = pd.DataFrame({n: [] for n in names}, index=[])

    def append_data(self, time_stamp, data_dict):
        """
        Add data to the DataMuggler.

        Parameters
        ----------
        time_stamp : datetime or list of datetime
            The times of the data

        data_dict : dict
            The keys must be a sub-set of the columns that the DataMuggler
            knows about.  If `time_stamp` is a list, then the values must be
            lists of the same length, if `time_stamp` is a single datatime
            object then the values must be single values
        """
        if not all(k in self._dataframe for k in data_dict):
            # TODO dillify this error checking
            raise ValueError("trying to pass in invalid key")
        try:
            iter(time_stamp)
        except TypeError:
            # if time_stamp is not iterable, assume is a datime object
            # and we only have one data point to deal with so up-convert
            time_stamp = [time_stamp, ]
            data_dict = {k: [v, ] for k, v in six.iteritems(data_dict)}

        # deal with none-scalar look up magic
        for k in data_dict:
            # if none-scalar shove tha data into the storage
            # and replace the data with the time stamp
            if k in self._is_not_scalar:
                for t, v in zip(time_stamp, data_dict[k]):
                    self._nonscalar_lookup[k][t] = v
                data_dict[k] = time_stamp

        # make a new data frame with the input data and append it to the
        # existing data
        self._dataframe = self._dataframe.append(
            pd.DataFrame(data_dict, index=time_stamp))
        self._dataframe.sort(inplace=True)
        # emit that we have new data!
        self.new_data.emit(list(data_dict))

    def __len__(self):
        pass

    def get_values(self, reference_column, other_columns, time_range=None):
        """
        Return a dictionary of data resampled (filled) to the times which have
        non-NaN values in the reference column

        Parameters
        ----------
        reference_column : str
            The 'master' column to get time stamps from

        other_columns : list of str
            A list of the other columns to return

        time_range : tuple or None
            Times to limit returned data to.  This is not implemented.

        Returns
        -------
        index : list
            Nominally the times of each of data points

        out_data : dict
            A dictionary of the
        """
        if time_range is not None:
            raise NotImplementedError("you can only get all data right now")

        # grab the times/index where the primary key has a value
        index = self._dataframe[reference_column].dropna().index
        # make output dictionary
        out_data = dict()
        # for the keys we care about
        for k in [reference_column, ] + other_columns:
            # pull out the DataSeries
            work_series = self._dataframe[k]
            # fill in the NaNs using what ever method needed
            work_series = work_series.fillna(method=self._fill_methods[k])
            # select it only at the times we care about
            work_series = work_series[index]
            # if it is not a scalar, do the look up
            if k in self._is_not_scalar:
                out_data[k] = [self._nonscalar_lookup[k][t]
                               for t in work_series]
            # else, just turn the series into a list so we have uniform
            # return types
            else:
                out_data[k] = list(work_series.values)

        # return the index an the dictionary
        return list(index), out_data

    def get_last_value(self, reference_column, other_columns):
        """
        Return a dictionary of the dessified row an the most recent
        time where reference column has a valid value

        Parameters
        ----------
        reference_column : str
            The 'master' column to get time stamps from

        other_columns : list of str
            A list of the other columns to return

        time_range : tuple or None
            Times to limit returned data to.  This is not implemented.

        Returns
        -------
        index : Timestamp
            The time associated with the data

        out_data : dict
            A dictionary of the
        """
        # grab the times/index where the primary key has a value
        index = self._dataframe[reference_column].dropna().index
        # make output dictionary
        out_data = dict()
        # for the keys we care about
        for k in [reference_column, ] + other_columns:
            # pull out the DataSeries
            work_series = self._dataframe[k]
            # fill in the NaNs using what ever method needed
            work_series = work_series.fillna(method=self._fill_methods[k])
            # select it only at the times we care about
            work_series = work_series[index]
            # if it is not a scalar, do the look up
            if k in self._is_not_scalar:
                out_data[k] = self._nonscalar_lookup[k][work_series.value[-1]]

            # else, just turn the series into a list so we have uniform
            # return types
            else:
                out_data[k] = list(work_series.values[-1])

        # return the index an the dictionary
        return list(index), out_data


class MuggleWatcherLatest(QtCore.QObject):
    """
    This is a class that watches DataMuggler's for the `new_data` signal, grabs
    the lastest
    """
    sig = QtCore.Signal(list, dict)

    def __init__(self, muggler, watch_column, extract_colums):
        self._muggler = muggler
        self._ref_col = watch_column
        self._other_cols = extract_colums
        self._muggler.new_data.connect(self.process_message)

    @QtCore.Slot(list)
    def process_message(self, updated_cols):
        """
        Process the updates from the muggler to see if there
        is anything we need to deal with.

        Parameters
        ----------
        updated_cols : list
            Updated columns

        """
        if self._ref_col in updated_cols:
            ind, res_dict = self._muggler.get_last_value(self._ref_col,
                                                         self._other_cols)
            self.sig.emit(ind, res_dict)


class MuggleWatcherTwoLists(QtCore.QObject):
    """

    """
    sig = QtCore.QSignal(list, list)

    def __init__(self, muggler, col1, col2):
        """
        Parameters
        ----------
        muggler : DataMuggler
            The data
        """
        self._muggler = muggler
        self._ref_col = col1
        self._other_cols = [col2,]
        self._muggler.new_data.connect(self.process_message)


class MuggleWatcherAll(QtCore.QObject):
    """
    This class watches a DataMunggler and when it gets new data extracts
    all of the time series data, not just the latest.
    """
    pass
