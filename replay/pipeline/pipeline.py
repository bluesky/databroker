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
from enaml.qt import QtCore
from collections import namedtuple
import pandas as pd
from datetime import datetime
import numpy as np
from pims.base_frames import FramesSequence
from pims.frame import Frame
from broker.client import read_json_from_socket
import time


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
            print(message, data_payload)
        else:
            if ret is not None:
                self.source_signal.emit(*ret)


class Unalignable(Exception):
    """
    An exception to raise if you try to align a non-fillable column
    to a non-pre-aligned column
    """
    pass


class ColSpec(namedtuple('ColSpec', ['name', 'fill_method', 'dims'])):
    """
    Named-tuple sub-class to validate the column specifications for the
    DataMuggler

    Parameters
    ----------
    name : hashable
    fill_method : {'pad', 'ffill', None}
        None means that no filling is done
    dims : uint
        Dimensionality of the data stored in the column
    """
    # removed the back-fill ones (even though pandas allows them)
    valid_fill_methods = {'pad', 'ffill', None, 'bfill', 'backpad'}

    __slots__ = ()

    def __new__(cls, name, fill_method, dims):
        # sanity check dims
        if int(dims) < 0:
            raise ValueError("Dims must be positive not {}".format(dims))

        # sanity check fill_method
        if fill_method not in cls.valid_fill_methods:
            raise ValueError("{} is not a valid fill method must be one of "
                                 "{}".format(fill_method,
                                             cls.valid_fill_methods))

        # pass everything up to base class
        return super(ColSpec, cls).__new__(cls, name, fill_method, dims)


class DataMuggler(QtCore.QObject):
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

    """

    # this is a signal emitted when the muggler has new data that clients
    # can grab.  The names of the columns that have new data are emitted
    # as a list
    new_data = QtCore.Signal(list)

    # this is a signal emitted when the muggler has new data sets that clients
    # can grab . The names of the new columns are emitted as a list
    new_columns = QtCore.Signal(list)

    def __init__(self, col_info, **kwargs):
        super(DataMuggler, self).__init__(**kwargs)
        self.recreate_columns(col_info)

    def recreate_columns(self, col_info):
        """
        Recreate the columns with new column information.  This
        implies a clear and the muggler in empty with the new columns
        after this call.

        Parameters
        ----------
        col_info : list
           List of information about the columns. Each entry should
           be a tuple of the form (col_name, fill_method, dimensionality). See
           `ColSpec` class docstring

        """
        # validate column spec
        self._col_info = [ColSpec(*c) for c in col_info]
        self.clear()
        self.new_columns.emit(self.keys())

    def clear(self):
        """
        Clear all of the data by re-initializing all of the internal
        data structures.
        """
        self._col_fill = dict()
        self._nonscalar_col_lookup = dict()
        self._is_col_nonscalar = set()
        names = []
        for ci in self._col_info:
            # used to sort out which way filling should be done.
            # forward for motor-like, backwards from image-like
            self._col_fill[ci.name] = ci.fill_method
            # determine if the value should be stored directly in the data
            # frame or in a separate data structure
            if ci.dims > 0:
                self._is_col_nonscalar.add(ci.name)
                self._nonscalar_col_lookup[ci.name] = dict()
            names.append(ci.name)

        # make an empty data frame
        self._dataframe = pd.DataFrame({n: [] for n in names}, index=[])

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
    def col_fill_rules(self):
        """
        Fill rules for all of the columns.
        """
        return {c.name: c.fill_method for c in self._col_info}

    def align_against(self, ref_col, other_cols=None):
        """
        Determine what columns can be sliced against another column.

        This matters because not all columns can be filled and would
        result in getting back non-dense events.

        Currently this just decides based on if the column can be filled,
        but this might need to be made smarter to deal with synchronous
        collection of multiple un-fillable measurements.

        Parameters
        ----------
        ref_col : str
            The name of the proposed reference column
        other_cols : list
            The names of the columns to test for alignment

        Returns
        -------
        dict
            Keyed on column name, True if that column can be sliced at
            the times of the input column.
        """
        if ref_col not in self._dataframe:
            raise ValueError("non-existent columnn: [[{}]]".format(ref_col))
        ref_index = self._dataframe[ref_col].dropna().index
        tmp_dict = {}
        for col_name, col_fill_type in six.iteritems(self._col_fill):
            if col_name == ref_col:
                tmp_dict[col_name] = True
            elif other_cols and col_name not in other_cols:
                # skip column names that are not in other_cols, if it passed in
                continue
            elif col_fill_type is None:
                tmp_dict[col_name] = False
            else:
                filled = self._dataframe[col_name].fillna(method=col_fill_type)
                algnable = filled[ref_index].notnull().all()
                tmp_dict[col_name] = bool(algnable)
        return tmp_dict

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
            k_dataframe = set(list(self._dataframe.columns.values))
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
        df, new = self._dataframe.align(
            pd.DataFrame(data_dict, index=time_stamp))
        df.update(new)
        self._dataframe = df
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
            raise NotImplementedError("t_start is not implemented. You can "
                                      "only get all data right now")
        if t_finish is not None:
            raise NotImplementedError("t_finish is not implemented. You can "
                                      "only get all data right now")
        cols = list(set(other_cols + [ref_col, ]))
        index = self._dataframe[ref_col].dropna().index
        dense_table = self._densify_sub_df(cols)
        reduced_table = dense_table.loc[index]
        out_index, out_data = self._lookup_non_scalar(reduced_table)
        # return the times/indices and the dictionary
        return out_index, out_data

    def get_column(self, col_name):
        """
        Return the time and values where the given column is non-nan

        Parameters
        ----------
        col_name : str
            The name of the column to return

        Returns
        -------
        time : array-like
            The time stamps of the non-nan values

        out_vals : array-like
            The values at those times
        """
        if col_name not in self._dataframe:
            raise ValueError(("The column {} does not exist. "
                              "Possible values are {}").format(
                                  col_name, self.keys()))

        out_series = self._dataframe[col_name].dropna()
        time = out_series.index.values
        out_vals = out_series.values
        if col_name in self._is_col_nonscalar:
            out_vals = [self._nonscalar_col_lookup[col_name][t]
                               for t in out_vals]

        return time, out_vals

    def get_times(self, col):
        """
        Return the time stamps that a column has non-null data
        at.


        Parameters
        ----------
        col : str
            The name of the column to extract the times for.
        """
        return self._dataframe[col].dropna().index

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
        cols = list(set(other_cols + [ref_col, ]))

        # grab the times/index where the primary key has a value
        index = self._dataframe[ref_col].dropna().index
        dense_table = self._densify_sub_df(cols)
        reduced_table = dense_table.loc[index[-1:]]
        out_index, data = self._lookup_non_scalar(reduced_table)
        return out_index[-1], {k: v[0] for k, v in six.iteritems(data)}

    def get_row(self, index, cols):
        """
        Return a row with the selected columns
        """
        # this should be made a bit more clever to only look at region
        # around the row we care about, not _everything_
        dense_array = self._densify_sub_df(cols)
        row = dense_array.loc[index]
        out_dict = dict()
        for k, v in zip(row.index, row):
            if k in self._is_col_nonscalar:
                out_dict[k] = self._nonscalar_col_lookup[k][v]
            else:
                out_dict[k] = v

        return out_dict

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

    def __iter__(self):
        return iter(self._dataframe)

    def _densify_sub_df(self, col_names, index=None):
        """
        Internal function to fill and hack-down the data frame-as-needed

        Parameters
        ----------
        col_names : list
             List of strings naming the columns to extract

        index : pandas index or None
            If None, do whole frame, else, only work on the
            subset specified by index.  This is applied _before_ filling
            so this should be a continious range (or mask out rows you don't
            want included) _not_ for reducing the result to the times based
            on a reference column.

        Returns
        -------
        DataFrame
            A filled data frame
        """
        tmp_data = dict()
        if index is not None:
            work_df = self._dataframe[index]
        else:
            work_df = self._dataframe
        for col in col_names:
            # grab the column
            work_series = work_df[col]
            # fill in the NaNs using what ever method needed
            if self._col_fill[col] is not None:
                work_series = work_series.fillna(
                    method=self._col_fill[col])
            tmp_data[col] = work_series
        return pd.DataFrame(tmp_data)

    def _lookup_non_scalar(self, df):
        """
        Given a data frame (which is assumed to be a hacked-down
        version of self._dataframe which has been densified)

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
        index : pandas.core.index.Index
            The index of the data frame
        data : dict
            Dictionary keyed on column name of the column.  The value is
            one of (ndarray, list, pd.Series)
        """
        ret_dict = dict()

        for col in df:
            ws = df[col]
            if ws.isnull().any():
                print(col)
                print(ws)
                raise Unalignable("columns aren't aligned correctly")

            if col in self._is_col_nonscalar:
                lookup_dict = self._nonscalar_col_lookup[col]
                ret_dict[col] = [lookup_dict[t] for t in ws]
            else:
                ret_dict[col] = ws
        return df.index, ret_dict


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


class DmImgSequence(FramesSequence):
    """
    This is a PIMS class for dealing with images stored in a DataMuggler.

    Parameters
    ----------
    dm : DataMuggler
        Where to get the data from
    """
    @classmethod
    def class_exts(cls):
        # does not do files
        return set()

    def __init__(self, data_muggler, data_name, image_shape=None,
                 process_func=None, dtype=None, as_grey=False):
        # stash the DataMuggler
        self._data_muggler = data_muggler
        # stash the column we care about
        self._data_name = data_name
        # assume is floats (for now)
        self._pixel_type = np.float
        # frame shape is passed in
        if image_shape is None:
            image_shape = (1, 1)
        self._image_shape = image_shape

        self._validate_process_func(process_func)
        self._as_grey(as_grey, process_func)

    @property
    def data_name(self):
        return self._data_name

    @property
    def data_muggler(self):
        return self._data_muggler

    @property
    def frame_shape(self):
        return self._image_shape

    @property
    def pixel_type(self):
        return self._pixel_type

    def get_frame(self, n):
        time = self._data_muggler.get_times(self.data_name)
        data = self._data_muggler.get_row(time[n], [self.data_name, ])
        raw_data = data[self.data_name]
        self._image_shape = raw_data.shape
        return Frame(self.process_func(raw_data).astype(self._pixel_type),
                     frame_no=n)

    def __len__(self):
        return len(self._data_muggler.get_times(self.data_name))

    def __repr__(self):
        state = "Current state of DmImgSequence object"
        state += "\nData Muggler: {}".format(self._data_muggler)
        state += "\nData Name: {}".format(self._data_name)
        state += "\nPixel Type: {}".format(self._pixel_type)
        state += "\nImage Shape: {}".format(self._image_shape)
        return state


class SocketWorker(QtCore.QObject):
    """
    Worker that pings the broker stream server

    Parameters
    ----------
    bss_name : str
        host name or host ip address for the broker stream server
    bss_port : int
        port to connect to the broker stream server on
    parent : QtCore.QObject, optional

    Attributes
    ----------
    event : QtCore.Signal
        Signal that emits an object and a dictionary. Ostensibly the object is
        a datetime object and the dict is something that a data muggler will
        understand
    read : QtCore.Signal
        Signal that indicates the data from the socket was successfully acquired
    """
    event = QtCore.Signal(object, dict)
    read_finished = QtCore.Signal()

    def __init__(self, bss_name, bss_port, parent=None):
        QtCore.QObject.__init__(self, parent)
        self.host_name = bss_name
        self.host_port = bss_port

    def read_socket(self):
        """ Read data from the broker stream server
        """
        time_stamp, data = read_json_from_socket(self.host_name,
                                                 self.host_port)[0]
        time_stamp = datetime.utcnow()
        print('{}: {}'.format(time_stamp, data))
        self.event.emit(time_stamp, data)
        self.read_finished.emit()


class SocketListener(QtCore.QObject):
    """
    Listener that a user can hook in to for the purpose of gathering live data

    Parameters
    ----------
    bss_name : str
        host name or host ip address for the broker stream server
    bss_port : int
        port to connect to the broker stream server on
    parent : QtCore.QObject, optional

    Attributes
    ----------
    event : QtCore.Signal
        Signal that emits an object and a dictionary. Ostensibly the object is
        a datetime object and the dict is something that a data muggler will
        understand
    trigger : QtCore.Signal

    Notes
    -----
    For the interaction between the external user, the `SocketListener` and the
    `SocketWorker` see /doc/diagram/SocketListener.png
    """
    event = QtCore.Signal(object, dict)
    trigger = QtCore.Signal()
    _is_alive = True

    def __init__(self, bss_name, bss_port, parent=None, **kwargs):
        QtCore.QObject.__init__(self, parent=parent, **kwargs)
        self.svr_name = bss_name
        self.svr_port = bss_port
        self.worker = SocketWorker(self.svr_name, self.svr_port)

        self.thread = QtCore.QThread(parent=self)
        self.worker.moveToThread(self.thread)

        self.worker.event.connect(self.event.emit)

        self.worker.event.connect(self.event.emit)
        self.worker.read_finished.connect(self._feedback)
        self.trigger.connect(self.worker.read_socket)
        self.thread.start()

    def start(self):
        """Start popping data off of the broker stream server
        """
        self.trigger.emit()

    def set_alive(self, alive, restart=False):
        """
        Allow the loop to proceed no further if `alive` is False (after the
        current loop, of course)

        Parameters
        ----------
        is_alive : bool
            true: remove the block from the feedback loop and restart the loop
                  if autostart is True
            false: put a block on feedback() so that the loop stops execution
        """
        self._is_alive = alive
        # restart the loop
        if restart and self._is_alive:
            self._feedback()

    def _feedback(self):
        """Trigger the worker thread to try to read data from the socket
        """
        time.sleep(0.1)
        if self._is_alive:
            self.trigger.emit()
