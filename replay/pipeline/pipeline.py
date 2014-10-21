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
from datetime import datetime


class PipelineComponent(QtCore.QObject):
    """
    The top-level object to represent a component in the quick-and-dirty
    live-data pipe line.

    WARNING these docs do not match what is written, but are what _should_
    be written.  Currently the message is serving as the index which should
    probably be packed in with the data or as it own argument.  In either
    case we need to talk to the controls group before spending the effort to
    re-factor.

    This class provides the basic machinery for the signal and
    slot required for the hooking up the pipeline.

    The processing function needs to be provided at instantiation:

        def process_msg(message, data_object):
            # do stuff with message/data
            return result_message, result_data

    This function can also return `None` to indicate that there is no
    output for further processing.

    The schema for managing the content of the messages will be pinned down
    at a later date.

    Currently this scheme does not type-check to ensure that the pipeline
    connections are correct or valid before the pipeline is executed

    Parameters
    ----------
    process_function : callable
        Must have the following signature:

        def process_msg(message, data_object):
            # do stuff with message/data
            return result_message, result_data
            # return None

    """
    source_signal = QtCore.Signal(object, object)

    def __init__(self, process_function, **kwargs):
        super(PipelineComponent, self).__init__(**kwargs)
        self._process_msg = process_function

    @QtCore.Slot(object, object)
    def sink_slot(self, message, data_payload):
        """
        This function is the entry point for pushing data through
        this node in the pipeline.

        Parameters
        ----------
        timestamp : datetime or sequence of datetime
            The timestamp that aligns with the data in the data payload

        data_payload : object
            dict of lists or something that looks like it.


        """
        try:
            ret = self._process_msg(message, data_payload)
        except Exception as E:
            # yes, gotta catch 'em all!!
            print("something failed")
            print(E)
        else:
            if ret is not None:
                self.source_signal.emit(*ret)


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
    col_info : list
        List of information about the columns. Each entry should
        be a tuple of the form (col_name, fill_method, is_scalar)

    """

    # this is a signal emitted when the muggler has new data that clients
    # can grab.  The names of the columns that have new data are returned
    # as a list
    new_data = QtCore.Signal(list)

    # this is the function that gets called to validate that the row labels
    # are something that the internal pandas dataframe will understand
    _time_validator = datetime

    def __init__(self, col_info, **kwargs):
        super(DataMuggler, self).__init__(**kwargs)
        valid_fill_methods = {'pad', 'ffill', 'bfill', 'backpad'}

        self._col_fill = dict()
        self._nonscalar_col_lookup = dict()
        self._is_col_nonscalar = set()
        names = []
        for col_name, fill_method, is_scalar in col_info:
            # validate fill methods
            if fill_method not in valid_fill_methods:
                raise ValueError("{} is not a valid fill method must be one of "
                                 "{}".format(fill_method, valid_fill_methods))
            # used to sort out which way filling should be done.
            # forward for motor-like, backwards from image-like
            self._col_fill[col_name] = fill_method
            # determine if the value should be stored directly in the data
            # frame or in a separate data structure
            if not is_scalar:
                self._is_col_nonscalar.add(col_name)
                self._nonscalar_col_lookup[col_name] = dict()
            names.append(col_name)

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
            k_dataframe = set(list(six.iterkeys(dataframe)))
            k_input = set(list(six.iterkeys(data_dict)))
            bogus_keys = k_input - k_dataframe
            raise ValueError('Passing in a key that the dataframe doesn\'t '
                             'know about. Key(s): {}'.format(bogus_keys))
        try:
            iter(time_stamp)
        except TypeError:
            # if time_stamp is not iterable, assume it is a datetime object
            # and we only have one data point to deal with so up-convert
            time_stamp = [time_stamp, ]
            data_dict = {k: [v, ] for k, v in six.iteritems(data_dict)}

        # TODO time step validation:
        # A better way to do this to make the data frame and then check that
        # the index it time like, then do something like this to sort out
        # which ones are bad.

        # deal with non-scalar look up magic
        for k in data_dict:
            # if non-scalar shove tha data into the storage
            # and replace the data with the id of the value object
            # this should probably be a hash, but this is quick and dirty
            if k in self._is_col_nonscalar:
                ids = []
                for v in data_dict[k]:
                    ids.append(id(v))
                    self._nonscalar_col_lookup[k][id(v)] = v
                data_dict[k] = ids

        # make a new data frame with the input data and append it to the
        # existing data
        self._dataframe = self._dataframe.append(
            pd.DataFrame(data_dict, index=time_stamp))
        self._dataframe.sort(inplace=True)
        # emit that we have new data!
        self.new_data.emit(list(data_dict))

    def get_values(self, ref_col, other_cols, t_start=None, t_finish=None):
        """
        Return a dictionary of data resampled (filled) to the times which have
        non-NaN values in the reference column

        Parameters
        ----------
        ref_col : str
            The name of the 'master' column to get time stamps from

        other_cols : list of str
            A list of column names to return data from

        t_start : datetime or None
            Start time to obtain data for. This is not implemented

        t_finish : datetime or None
            End time to obtain data for. This is not implemented

        Returns
        -------
        indices : list
            Nominally the times of each of data points

        out_data : dict
            A dictionary of the data keyed on the column name with values
            as lists whose length is the same as 'indices'
        """
        if t_start is not None:
            raise NotImplementedError("t_start is not implemented. You can only "
                                      "get all data right now")
        if t_finish is not None:
            raise NotImplementedError("t_finish is not implemented. You can "
                                      "only get all data right now")

        # drop duplicate keys
        other_cols = list(set(other_cols))
        # grab the times/indices where the primary key has a value
        indices = self._dataframe[ref_col].dropna().index
        # make output dictionary
        out_data = dict()
        # get data only for the keys we care about
        for k in [ref_col, ] + other_cols:
            # pull out the pandas.Series
            working_series = self._dataframe[k]
            # fill in the NaNs using what ever method needed
            working_series = working_series.fillna(method=self._col_fill[k])
            # select it only at the times we care about
            working_series = working_series[indices]
            # if it is not a scalar, do the look up
            if k in self._is_col_nonscalar:
                out_data[k] = [self._nonscalar_col_lookup[k][t]
                               for t in working_series]
            # else, just turn the series into a list so we have uniform
            # return types
            else:
                out_data[k] = list(working_series.values)

        # return the times/indices and the dictionary
        return list(indices), out_data

    def get_last_value(self, ref_col, other_cols):
        """
        Return a dictionary of the dessified row and the most recent
        time where reference column has a valid value

        Parameters
        ----------
        ref_col : str
            The name of the 'master' column to get time stamps from

        other_cols : list of str
            A list of column names to return data from

        Returns
        -------
        index : Timestamp
            The time associated with the data

        out_data : dict
            A dictionary of the data keyed on the column name with values
            as lists whose length is the same as 'indices'
        """
        # drop duplicate keys
        other_cols = list(set(other_cols))
        # grab the times/index where the primary key has a value
        index = self._dataframe[ref_col].dropna().index
        # make output dictionary
        out_data = dict()
        # for the keys we care about
        for k in [ref_col, ] + other_cols:
            # pull out the pandas.Series
            work_series = self._dataframe[k]
            # fill in the NaNs using what ever method needed
            work_series = work_series.fillna(method=self._col_fill[k])
            # select it only at the times we care about
            work_series = work_series[index]
            # if it is not a scalar, do the look up
            if k in self._is_col_nonscalar:
                out_data[k] = self._nonscalar_col_lookup[k][work_series.values[-1]]

            # else, just turn the series into a list so we have uniform
            # return types
            else:
                out_data[k] = work_series.values[-1]

        # return the time and the dictionary
        return index[-1], out_data


class MuggleWatcherLatest(QtCore.QObject):
    """
    This is a class that watches a DataMuggler for the `new_data` signal, grabs
    the lastest row (filling in data from other rows as needed) for the
    selected columns.  You probably should not extract columns which fill back
    as they will come out as NaN (I think).

    Parameters
    ----------
    muggler : DataMuggler
        The muggler to keep tabs on

    ref_col : str
        The name of the 'master' column to watch for new data

    other_cols : list of str
        A list of column names to return data from in addition to 'ref_col'
    """

    # signal to emit index (time) + data
    sig = QtCore.Signal(object, dict)

    def __init__(self, muggler, ref_col, other_cols, **kwargs):
        super(MuggleWatcherLatest, self).__init__(**kwargs)
        self._muggler = muggler
        self._ref_col = ref_col
        self._other_cols = other_cols
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
            indices, results_dict = self._muggler.get_last_value(
                self._ref_col, self._other_cols)
            self.sig.emit(indices, results_dict)


class MuggleWatcherTwoLists(QtCore.QObject):
    """
    This class watches a DataMuggler and when it gets new data extracts
    all of the time series data, for two columns and emits both as lists

    Parameters
    ----------
    muggler : DataMuggler
        The muggler to keep tabs on

    ref_col : str
        The name of the 'master' column to watch for new data

    col1 : str
        The name of the first extra column to extract data from when ref_col
        gets pinged

    col2 : str
        The name of the second extra column to extract data from when ref_col
        gets pinged
    """
    # Signal that emits lists of two datasets
    sig = QtCore.Signal(list, list)

    def __init__(self, muggler, ref_col, col1, col2, **kwargs):
        super(MuggleWatcherTwoLists, self).__init__(**kwargs)
        self._muggler = muggler
        self._ref_col = ref_col
        self._other_cols = [col1, col2]
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
            ind, res_dict = self._muggler.get_values(self._ref_col,
                                                         self._other_cols)
            self.sig.emit(res_dict[self._other_cols[0]],
                          res_dict[self._other_cols[1]])


class MuggleWatcherAll(QtCore.QObject):
    """
    This class watches a DataMuggler and when it gets new data extracts
    all of the time series data, not just the latest.

    Parameters
    ----------
    muggler : DataMuggler
        The muggler to keep tabs on

    ref_col : str
        The name of the 'master' column to watch for new data

    other_cols : list of str
        The other columns to extract data from when ref_col gets pinged with
        new data
    """
    sig = QtCore.Signal(list, dict)

    def __init__(self, muggler, ref_col, other_cols, **kwargs):
        super(MuggleWatcherAll, self).__init__(**kwargs)
        self._muggler = muggler
        self._ref_col = ref_col
        self._other_cols = other_cols
        self._muggler.new_data.connect(self.process_message)

    @QtCore.Slot(list)
    def process_message(self, updated_cols):
        """
        Process the updates from the muggler to see if there
        is anything we need to deal with.

        Parameters
        ----------
        updated_cols : list
            Columns that were updated in the data muggler

        """
        if self._ref_col in updated_cols:
            ind, res_dict = self._muggler.get_values(self._ref_col,
                                                     self._other_cols)
            self.sig.emit(ind, res_dict)
