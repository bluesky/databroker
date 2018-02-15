from __future__ import (absolute_import, division, print_function,
                        unicode_literals)
from . import core
import numpy as np


class MDSROTemplate(object):
    _API_MAP = {1: core}

    def __init__(self, config):
        self._RUNSTART_CACHE = {}
        self._RUNSTOP_CACHE = {}
        self._DESCRIPTOR_CACHE = {}
        self.config = config
        self._api = None
        self.version = config.get('version', 1)

    def reset_caches(self):
        self._RUNSTART_CACHE.clear()
        self._RUNSTOP_CACHE.clear()
        self._DESCRIPTOR_CACHE.clear()

    def __getstate__(self):
        return self.version, self.config

    def __setstate__(self, state):
        self._RUNSTART_CACHE = {}
        self._RUNSTOP_CACHE = {}
        self._DESCRIPTOR_CACHE = {}
        self._api = None
        self.version, self.config = state

    @property
    def version(self):
        return self._version

    @version.setter
    def version(self, val):
        if self._api is not None:
            raise RuntimeError("Can not change api version at runtime")
        self._api = self._API_MAP[val]
        self._version = val

    @property
    def NoRunStart(self):
        return self._api.NoRunStart

    @property
    def NoRunStop(self):
        return self._api.NoRunStop

    @property
    def NoEventDescriptors(self):
        return self._api.NoEventDescriptors

    def reconfigure(self, config):
        self.config = config

    def clear_process_cache(self):
        """Clear all local caches"""
        self._RUNSTART_CACHE.clear()
        self._RUNSTOP_CACHE.clear()
        self._DESCRIPTOR_CACHE.clear()

    def run_start_given_uid(self, uid):
        """Given a uid, return the RunStart document

        Parameters
        ----------
        uid : str
            The uid

        Returns
        -------
        run_start : doc.Document
            The RunStart document.

        """
        return self._api.run_start_given_uid(uid, self._runstart_col,
                                             self._RUNSTART_CACHE)

    def run_stop_given_uid(self, uid):
        """Given a uid, return the RunStop document

        Parameters
        ----------
        uid : str
            The uid

        Returns
        -------
        run_stop : doc.Document
            The RunStop document.

        """
        return self._api.run_stop_given_uid(uid,
                                            self._runstop_col,
                                            self._RUNSTOP_CACHE)

    def descriptor_given_uid(self, uid):
        """Given a uid, return the EventDescriptor document

        Parameters
        ----------
        uid : str
            The uid

        Returns
        -------
        descriptor : doc.Document
            The EventDescriptor document fully de-referenced
        """
        return self._api.descriptor_given_uid(uid, self._descriptor_col,
                                              self._DESCRIPTOR_CACHE)

    def stop_by_start(self, run_start):
        """Given a RunStart return it's RunStop

        Raises if no RunStop exists.

        Parameters
        ----------
        run_start : doc.Document or dict or str
            The RunStart to get the RunStop for.  Can be either
            a Document/dict with a 'uid' key or a uid string

        Returns
        -------
        run_stop : doc.Document
            The RunStop document

        Raises
        ------
        NoRunStop
            If no RunStop document exists for the given RunStart
        """
        return self._api.stop_by_start(run_start,
                                       self._runstop_col,
                                       self._RUNSTOP_CACHE)

    def descriptors_by_start(self, run_start):
        """Given a RunStart return a list of it's descriptors

        Raises if no EventDescriptors exist.

        Parameters
        ----------
        run_start : doc.Document or dict or str
            The RunStart to get the EventDescriptors for.  Can be either
            a Document/dict with a 'uid' key or a uid string

        Returns
        -------
        event_descriptors : list
            A list of EventDescriptor documents

        Raises
        ------
        NoEventDescriptors
            If no EventDescriptor documents exist for the given RunStart
        """
        return self._api.descriptors_by_start(run_start,
                                              self._descriptor_col,
                                              self._DESCRIPTOR_CACHE)

    def get_events_generator(self, descriptor, convert_arrays=True):
        """A generator which yields all events from the event stream

        Parameters
        ----------
        descriptor : doc.Document or dict or str
            The EventDescriptor to get the Events for.  Can be either
            a Document/dict with a 'uid' key or a uid string
        convert_arrays : boolean
            convert 'array' type to numpy.ndarray; True by default

        Yields
        ------
        event : doc.Document
            All events for the given EventDescriptor from oldest to
            newest
        """
        evs = self._api.get_events_generator(descriptor,
                                             self._event_col,
                                             self._descriptor_col,
                                             self._DESCRIPTOR_CACHE,
                                             self._runstart_col,
                                             self._RUNSTART_CACHE,
                                             convert_arrays=convert_arrays)

        # when we drop 2.7, this can be
        # yield from evs
        for ev in evs:
            yield ev

    def get_events_table(self, descriptor):
        """All event data as tables

        Parameters
        ----------
        descriptor : dict or str
            The EventDescriptor to get the Events for.  Can be either
            a Document/dict with a 'uid' key or a uid string

        Returns
        -------
        descriptor : doc.Document
            EventDescriptor document
        data_table : dict
            dict of lists of the transposed data
        seq_nums : list
            The sequence number of each event.
        times : list
            The time of each event.
        uids : list
            The uid of each event.
        timestamps_table : dict
            The timestamps of each of the measurements as dict of lists.  Same
            keys as `data_table`.
        """
        return self._api.get_events_table(descriptor,
                                          self._event_col,
                                          self._descriptor_col,
                                          self._DESCRIPTOR_CACHE,
                                          self._runstart_col,
                                          self._RUNSTART_CACHE)

    def find_run_starts(self, **kwargs):
        """Given search criteria, locate RunStart Documents.

        Parameters
        ----------
        since : time-like, optional
            time-like representation of the earliest time that a RunStart
            was created. Valid options are:
               - timestamps --> time.time()
               - '2015'
               - '2015-01'
               - '2015-01-30'
               - '2015-03-30 03:00:00'
               - datetime.datetime.now()
        until : time-like, optional
            timestamp of the latest time that a RunStart was created. See
            docs for `since` for examples.
        beamline_id : str, optional
            String identifier for a specific beamline
        project : str, optional
            Project name
        owner : str, optional
            The username of the logged-in user when the scan was performed
        scan_id : int, optional
            Integer scan identifier

        Returns
        -------
        rs_objects : iterable of doc.Document objects


        Examples
        --------
        >>> find_run_starts(scan_id=123)
        >>> find_run_starts(owner='arkilic')
        >>> find_run_starts(since=1421176750, until=time.time()})
        >>> find_run_starts(since=1421176750, until=time.time())

        >>> find_run_starts(owner='arkilic', since=1421176750.514707,
        ...                until=time.time())

        """
        gen = self._api.find_run_starts(self._runstart_col,
                                        self._RUNSTART_CACHE,
                                        self.config['timezone'],
                                        **kwargs)
        for rs in gen:
            yield rs

    def find_run_stops(self, **kwargs):
        """Given search criteria, locate RunStop Documents.

        Parameters
        ----------
        run_start : doc.Document or str, optional
            The RunStart document or uid to get the corresponding run end for
        since : time-like, optional
            time-like representation of the earliest time that a RunStop
            was created. Valid options are:
               - timestamps --> time.time()
               - '2015'
               - '2015-01'
               - '2015-01-30'
               - '2015-03-30 03:00:00'
               - datetime.datetime.now()
        until : time-like, optional
            timestamp of the latest time that a RunStop was created. See
            docs for `since` for examples.
        exit_status : {'success', 'fail', 'abort'}, optional
            provides information regarding the run success.
        reason : str, optional
            Long-form description of why the run was terminated.
        uid : str, optional
            Globally unique id string provided to metadatastore

        Yields
        ------
        run_stop : doc.Document
            The requested RunStop documents
        """
        gen = self._api.find_run_stops(self._runstop_col,
                                       self._RUNSTOP_CACHE,
                                       self.config['timezone'],
                                       **kwargs)
        for rs in gen:
            yield rs

    def find_descriptors(self, **kwargs):
        """Given search criteria, locate EventDescriptor Documents.

        Parameters
        ----------
        run_start : doc.Document or str, optional
            The RunStart document or uid to get the corresponding run end for
        since : time-like, optional
            time-like representation of the earliest time that an
            EventDescriptor was created. Valid options are:
               - timestamps --> time.time()
               - '2015'
               - '2015-01'
               - '2015-01-30'
               - '2015-03-30 03:00:00'
               - datetime.datetime.now()
        until : time-like, optional
            timestamp of the latest time that an EventDescriptor was created.
            See docs for `since` for examples.
        uid : str, optional
            Globally unique id string provided to metadatastore

        Yields
        -------
        descriptor : doc.Document
            The requested EventDescriptor
        """
        gen = self._api.find_descriptors(self._descriptor_col,
                                         self._DESCRIPTOR_CACHE,
                                         self.config['timezone'],
                                         **kwargs)
        for desc in gen:
            yield desc

    def find_last(self, num=1):
        """Locate the last `num` RunStart Documents

        Parameters
        ----------
        num : integer, optional
            number of RunStart documents to return, default 1

        Yields
        ------
        run_start doc.Document
           The requested RunStart documents
        """

        for ev in self._api.find_last(self._runstart_col,
                                      self._RUNSTART_CACHE,
                                      num=num):
            yield ev


class MDSTemplate(MDSROTemplate):
    _INS_METHODS = {'start': 'insert_run_start',
                    'stop': 'insert_run_stop',
                    'descriptor': 'insert_descriptor',
                    'event': 'insert_event',
                    'bulk_events': 'bulk_insert_events'}

    def insert_run_start(self, time, uid, **kwargs):
        '''Insert a Start document

        All extra keyword arguments are passed through to the database
        as fields in the Start document.

        Parameters
        ----------
        time : float
            The date/time as found at the client side when the run is started
        uid : str
            Globally unique id to identify this RunStart
        scan_id : int, optional
            Scan identifier visible to the user and data analysis.  This is not
            a unique identifier.
        owner : str, optional
            A username associated with the RunStart
        group : str, optional
            An experimental group associated with the RunStart
        project : str, optional
            Any project name to help users locate the data
        sample : str or dict, optional
        kwargs
            passed through

        Returns
        -------
        run_start : str
            uid of the inserted document.  Use `run_start_given_uid` to get
            the full document.
        '''
        return self._api.insert_run_start(self._runstart_col,
                                          self._RUNSTART_CACHE,
                                          time=time,
                                          uid=uid,
                                          **kwargs)

    def insert_run_stop(self, run_start, time, uid, exit_status='success',
                        reason='', **kwargs):
        """Insert RunStop document into database

        Parameters
        ----------
        run_start : doc.Document or dict or str
            The RunStart to insert the RunStop for.  Can be either
            a Document/dict with a 'uid' key or a uid string
        time : float
            The date/time as found at the client side
        uid : str
            Globally unique id string provided to metadatastore
        exit_status : {'success', 'abort', 'fail'}, optional
            indicating reason run stopped, 'success' by default
        reason : str, optional
            more detailed exit status (stack trace, user remark, etc.)

        Returns
        -------
        run_stop : str
            uid of inserted Document

        Raises
        ------
        RuntimeError
            Only one RunStop per RunStart, raises if you try to insert a second
        """
        return self._api.insert_run_stop(self._runstop_col,
                                         self._RUNSTOP_CACHE,
                                         run_start=run_start,
                                         time=time, uid=uid,
                                         exit_status=exit_status,
                                         reason=reason, **kwargs)

    def insert_descriptor(self, run_start, data_keys, time, uid, **kwargs):
        """Insert an EventDescriptor document in to database.

        Parameters
        ----------
        run_start : doc.Document or dict or str
            The RunStart to insert a Descriptor for.  Can be either
            a Document/dict with a 'uid' key or a uid string
        data_keys : dict
            Provides information about keys of the data dictionary in
            an event will contain.  No key name may include '.'.  See
            `DataKey` odm template for schema.
        time : float
            The date/time as found at the client side when an event
            descriptor is created.
        uid : str
            Globally unique id string provided to metadatastore

        Returns
        -------
        descriptor : str
            uid of inserted Document
        """
        return self._api.insert_descriptor(self._descriptor_col,
                                           self._DESCRIPTOR_CACHE,
                                           run_start=run_start,
                                           data_keys=data_keys,
                                           time=time, uid=uid,
                                           **kwargs)

    def insert_event(self, descriptor, time, seq_num, data, timestamps, uid,
                     validate=False, filled=None):
        """Create an event in metadatastore database backend

        .. warning

           This does not validate that the keys in `data` and `timestamps`
           match the data keys in `descriptor`.

        Parameters
        ----------
        descriptor : doc.Document or dict or str
            The Descriptor to insert event for.  Can be either
            a Document/dict with a 'uid' key or a uid string
        time : float
            The date/time as found at the client side when an event is
            created.
        seq_num : int
            Unique sequence number for the event. Provides order of an event in
            the group of events
        data : dict
            Dictionary of measured values (or external references)
        timestamps : dict
            Dictionary of measured timestamps for each values, having the
            same keys as `data` above
        uid : str
            Globally unique id string provided to metadatastore
        validate : boolean
            Check that data and timestamps have the same keys.
        filled : dict or None
            Dictionary of `False` or datum_ids. Keys are a subset of the keys
            in `data` and `timestamps` above.
        """
        if filled is None:
            filled = {}
        return self._api.insert_event(self._event_col,
                                      descriptor=descriptor,
                                      time=time, seq_num=seq_num,
                                      data=data,
                                      timestamps=timestamps,
                                      uid=uid,
                                      filled=filled,
                                      validate=validate)

    def bulk_insert_events(self, descriptor, events, validate=False):
        return self._api.bulk_insert_events(self._event_col,
                                            descriptor=descriptor,
                                            events=events,
                                            validate=validate)

    def insert(self, name, doc):
        if name != 'bulk_events':
            getattr(self, self._INS_METHODS[name])(**doc)
        else:
            for desc_uid, events in doc.items():
                # If events is empty, mongo chokes.
                if not events:
                    continue
                self.bulk_insert_events(desc_uid, events)
