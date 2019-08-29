from __future__ import (absolute_import, unicode_literals, generators)
import copy
import requests
from functools import wraps
import json
import six
import warnings
import numpy as np
import logging
from requests import HTTPError
from .mongo_core import (NoRunStart, NoEventDescriptors, NoRunStop,
                         BAD_KEYS_FMT)
from ..utils import sanitize_np, apply_to_dict_recursively

logger = logging.getLogger(__name__)


class MDSRO(object):
    """Read-only client for metadataservice

    Configuration is required to connect to a metadataservice server.
    host, port, timezone are needed. Timezone is consumed by databroker
    instances.

    Attributes
    ----------
    config: dict
        Configuration for the remote server.
    """
    def __init__(self, config):
        self._RUN_START_CACHE = {}
        self._RUNSTOP_CACHE = {}
        self._DESCRIPTOR_CACHE = {}
        self.config = config

    @property
    def NoRunStart(self):
        return NoRunStart

    @property
    def NoRunStop(self):
        return NoRunStop

    @property
    def NoEventDescriptors(self):
        return NoEventDescriptors

    @property
    def config(self):
        return self._config

    @config.setter
    def config(self, config):
        for key in ['host', 'port', 'timezone']:
            if key not in config:
                raise KeyError("The key {!r} is missing from the "
                               "configuration.".format(key))
        self._config = config

    @property
    def _server_path(self):
        """Base URL for metadataservice"""
        return "http://{}:{}/".format(self.config['host'],
                                      self.config['port']
                                      )

    @property
    def _rstart_url(self):
        """URL for RunStart handler"""
        return self._server_path + 'run_start'

    @property
    def _desc_url(self):
        """URL for EventDescriptor handler"""
        return self._server_path + 'event_descriptor'

    @property
    def _event_url(self):
        """URL for Event handler"""
        return self._server_path + 'event'

    @property
    def _rstop_url(self):
        """URL for RunStop handler"""
        return self._server_path + 'run_stop'

    def __get_hostname__(self):
        return self.hostname

    def cache_document(self, doc, doc_type, doc_cache):
        """Provided document type and cache dictionary,
        adds a document to the provided cache
        """
        doc = dict(doc)
        doc_cache[doc['uid']] = doc
        return doc

    def _cache_run_start(self, run_start, run_start_cache):
        """De-reference and cache a RunStart document

        The de-referenced Document is cached against the
        ObjectId and the uid -> ObjectID mapping is stored.

        Parameters
        ----------
        run_start : dict
            raw pymongo dictionary. This is expected to have
            an entry `_id` with the ObjectId used by mongo.

        Returns
        -------
        run_start : dict
            Document instance for this RunStart document.
            The ObjectId has been stripped.
        """
        return self.cache_document(run_start, 'RunStart', run_start_cache)

    def _cache_run_stop(self, run_stop, run_stop_cache):
        """De-reference and cache a RunStart document

        The de-referenced Document is cached against the
        ObjectId and the uid -> ObjectID mapping is stored.

        Parameters
        ----------
        run_start : dict
            raw pymongo dictionary. This is expected to have
            an entry `_id` with the ObjectId used by mongo.

        Returns
        -------
        run_start : dict
            Document instance for this RunStart document.
            The ObjectId has been stripped.
        """
        return self.cache_document(run_stop, 'RunStop', run_stop_cache)

    def _cache_descriptor(self, descriptor, descriptor_cache):
        return self.cache_document(descriptor, 'EventDescriptor',
                                   descriptor_cache)

    def doc_or_uid_to_uid(self, doc_or_uid):
        """Given Document or uid return the uid

        Parameters
        ----------
        doc_or_uid : dict or str
            If str, then assume uid and pass through, if not, return
            the 'uid' field

        Returns
        -------
        uid : str
            A string version of the uid of the given document

        """
        if not isinstance(doc_or_uid, six.string_types):
            doc_or_uid = doc_or_uid['uid']
        return doc_or_uid

    def reset_caches(self):
        self._RUN_START_CACHE.clear()
        self._RUNSTOP_CACHE.clear()
        self._DESCRIPTOR_CACHE.clear()

    clear_process_cache = reset_caches

    def queryfactory(self, query, signature):
        """
        Currently only returns a simple dict mdservice expects.
        This can be extended in the future
        """
        return dict(query=query, signature=signature)

    def _get(self, url, params):
        """Generic RESTful get for parsing query response from server"""
        r = requests.get(url, json.dumps(params))
        r.raise_for_status()
        return r.json()

    def run_start_given_uid(self, uid):
        """Given a uid, return the RunStart document
        Parameters
        ----------
        uid : str
            The uid
        Returns
        -------
        run_start : dict
            The RunStart document.
        """
        uid = self.doc_or_uid_to_uid(uid)
        try:
            return self._RUN_START_CACHE[uid]
        except KeyError:
            pass
        params = self.queryfactory(query={'uid': uid},
                                   signature='run_start_given_uid')
        response = self._get(self._rstart_url, params=params)
        if not response:
            raise NoRunStart('No RunStart found with uid {}'.format(uid))
        return response

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
        project : str, optional
            Project name
        owner : str, optional
            The username of the logged-in user when the scan was performed
        scan_id : int, optional
            Integer scan identifier
        Returns
        -------
        rs_objects : iterable of dicts
        Examples
        --------
        >>> find_run_starts(scan_id=123)
        >>> find_run_starts(owner='arkilic')
        >>> find_run_starts(since=1421176750.514707, until=time.time()})
        >>> find_run_starts(since=1421176750.514707, until=time.time())
        >>> find_run_starts(owner='arkilic', since=1421176750.514707,
        ...                until=time.time())
        """
        params = self.queryfactory(query=kwargs,
                                   signature='find_run_starts')
        response = self._get(self._rstart_url, params=params)
        for r in response:
            yield r

    def find_descriptors(self, **kwargs):
        """Given search criteria, locate EventDescriptor Documents.
        Parameters
        ----------
        run_start : dict or str, optional
        The RunStart document or uid to get the corresponding run end for
        since : time-like, optional
        time-like representation of the earliest time that an EventDescriptor
        was created. Valid options are:
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
        descriptor : dict
            The requested EventDescriptor
        """
        params = self.queryfactory(query=kwargs,
                                   signature='find_descriptors')
        response = self._get(self._desc_url, params=params)
        for r in response:
            yield r


    def find_run_stops(self, **kwargs):
        """Given search criteria, locate RunStop Documents.
        Parameters
        ----------
        run_start : dict or str, optional
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
        run_stop : dict
            The requested RunStop documents
        """
        params = self.queryfactory(query=kwargs,
                                   signature='find_run_stops')
        response = self._get(self._rstop_url, params=params)
        for r in response:
            yield r

    def run_stop_given_uid(self, uid):
        """Given a uid, return the RunStop document
        Parameters
        ----------
        uid : str
            The uid
        Returns
        -------
        run_stop : dict
            The RunStop document fully de-referenced
        """
        uid = self.doc_or_uid_to_uid(uid)
        try:
            return self._RUNSTOP_CACHE[uid]
        except KeyError:
            pass
        params = self.queryfactory(query={'uid': uid},
                                   signature='run_stop_given_uid')
        response = self._get(self._rstop_url, params=params)
        return response

    def descriptor_given_uid(self, uid):
        """Given a uid, return the EventDescriptor document
        Parameters
        ----------
        uid : str
            The uid
        Returns
        -------
        descriptor : dict
            The EventDescriptor document fully de-referenced
        """
        uid = self.doc_or_uid_to_uid(uid)
        params = self.queryfactory(query={'uid': uid},
                                   signature='descriptor_given_uid')
        response = self._get(self._desc_url, params=params)
        return response

    def descriptors_by_start(self, run_start):
        """Given a RunStart return a list of it's descriptors

        Raises if no EventDescriptors exist.

        Parameters
        ----------
        run_start : dict
            The RunStart to get the EventDescriptors for.  Can be either
            a dict with a 'uid' key or a uid string

        Returns
        -------
        event_descriptors : list
            A list of EventDescriptor documents

        Raises
        ------
        NoEventDescriptors
            If no EventDescriptor documents exist for the given RunStart
        """
        rstart_uid = self.doc_or_uid_to_uid(run_start)
        params = self.queryfactory(query={'run_start': rstart_uid},
                                   signature='descriptors_by_start')
        response = self._get(self._desc_url, params=params)
        if not response:
            raise NoEventDescriptors('No descriptor is found provided run_start {}'.format(rstart_uid))
        return response

    def stop_by_start(self, run_start):
        """Given a RunStart return it's RunStop

        Raises if no RunStop exists.

        Parameters
        ----------
        run_start : dict or str
            The RunStart to get the RunStop for.  Can be either
            a dict with a 'uid' key or a uid string

        Returns
        -------
        run_stop : dict
            The RunStop document

        Raises
        ------
        NoRunStop
            If no RunStop document exists for the given RunStart
        """
        uid = self.doc_or_uid_to_uid(run_start)
        params = self.queryfactory(query={'run_start': uid},
                                   signature='stop_by_start')
        response = self._get(self._rstop_url, params=params)
        return response
        # return self._cache_run_stop(response, self._RUNSTOP_CACHE)

    def get_events_generator(self, descriptor, convert_arrays=True):
        """A generator which yields all events from the event stream

        Parameters
        ----------
        descriptor : dict or str
            The EventDescriptor to get the Events for.  Can be either
            a dict with a 'uid' key or a uid string
        convert_arrays : boolean
            convert 'array' type to numpy.ndarray; True by default

        Yields
        ------
        event : dict
            All events for the given EventDescriptor from oldest to
            newest
        """
        descriptor_uid = self.doc_or_uid_to_uid(descriptor)
        params = self.queryfactory(query={'descriptor': descriptor_uid,
                                          'convert_arrays': convert_arrays},
                                   signature='get_events_generator')
        events = self._get(self._event_url, params=params)
        for e in events:
            yield e

    def get_events_table(self, descriptor):
        """All event data as tables

        Parameters
        ----------
        descriptor : dict or str
            The EventDescriptor to get the Events for.  Can be either
            a dict with a 'uid' key or a uid string

        Returns
        -------
        descriptor : dict
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
        desc_uid = self.doc_or_uid_to_uid(descriptor)
        descriptor = self.descriptor_given_uid(desc_uid)
        all_events = list(self.get_events_generator(descriptor=descriptor))
        seq_nums = [ev['seq_num'] for ev in all_events]
        times = [ev['time'] for ev in all_events]
        uids = [ev['uid'] for ev in all_events]
        keys = list(descriptor['data_keys'])
        data_table = self._transpose(all_events, keys, 'data')
        timestamps_table = self._transpose(all_events, keys, 'timestamps')
        return descriptor, data_table, seq_nums, times, uids, timestamps_table

    def _transpose(self, in_data, keys, field):
        """Turn a list of dicts into dict of lists
        Parameters
        ----------
        in_data : list
            A list of dicts which contain at least one dict.
            All of the inner dicts must have at least the keys
            in `keys`
        keys : list
            The list of keys to extract
        field : str
            The field in the outer dict to use
        Returns
        -------
        transpose : dict
            The transpose of the data
        """
        out = {k: [None] * len(in_data) for k in keys}
        for j, ev in enumerate(in_data):
            dd = ev[field]
            for k in keys:
                out[k][j] = dd[k]
        return out

    def find_last(self, num):
        """Locate the last `num` RunStart Documents
        Parameters
        ----------
        num : integer, optional
            number of RunStart documents to return, default 1
        Yields
        ------
        run_start : dict
        """
        params = self.queryfactory(query={'num': num},
                                   signature='find_last')
        response = self._get(self._rstart_url, params=params)
        for r in response:
            yield r


class MDS(MDSRO):
    """Read-write client for metadataservice"""
    def __init__(self, *args, **kwargs):
        """Extends the read-only client class"""
        super(MDS, self).__init__(*args, **kwargs)
        self._INS_METHODS = {'start': 'insert_run_start',
                             'stop': 'insert_run_stop',
                             'descriptor': 'insert_descriptor',
                             'event': 'insert_event',
                             'bulk_events': 'bulk_insert_events'}

    def datafactory(self, data, signature):
        """Pack data to be posted so mdservice can unpack"""
        if 'event' not in signature:
            # Events are sanitized with more care for perf, but other documents
            # need to be sanitized here.
            data = copy.deepcopy(data)
            apply_to_dict_recursively(data, sanitize_np)
        return dict(data=data, signature=signature)

    def _post(self, url, data):
        """Generic RESTful post"""
        r = requests.post(url, json.dumps(data))
        r.raise_for_status()
        return r.json()

    def insert(self, name, doc):
        """Generic insert for all docs"""
        if name != 'bulk_events':
            getattr(self, self._INS_METHODS[name])(**doc)
        else:
            for desc_uid, events in doc.items():
                # If events is empty, mongo chokes.
                if not events:
                    continue
                self.bulk_insert_events(desc_uid, events)

    def _check_for_custom(self, kdict):
        if 'custom' in kdict:
            warnings.warn("Custom is a deprecated field")
            custom = kdict.pop('custom')
            if any(k in kdict for k in custom):
                raise TypeError("Duplicate keys in kwargs and custom")
            kdict.update(custom)
        return kdict

    def insert_run_start(self, time, uid, **kwargs):
        """Insert a RunStart document into the database.
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
        Returns
        -------
        run_start : str
            uid of the inserted document.  Use `run_start_given_uid` to get
            the full document.
        """
        kwargs = self._check_for_custom(kwargs)
        doc = dict(time=time, uid=uid, **kwargs)
        data = self.datafactory(data=doc,
                                signature='insert_run_start')
        self._post(self._rstart_url, data=data)
        self._cache_run_start(doc,
                              self._RUN_START_CACHE)
        return uid

    def insert_run_stop(self, run_start, time, uid, exit_status='success',
                        reason=None,
                        **kwargs):
        """Insert RunStop document into database

        Parameters
        ----------
        run_start : dict or str
            The RunStart to insert the RunStop for.  Can be either
            a dict with a 'uid' key or a uid string
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
        kwargs = self._check_for_custom(kwargs)
        run_start_uid = self.doc_or_uid_to_uid(run_start)
        run_start = self.run_start_given_uid(run_start_uid)
        doc = dict(run_start=run_start_uid, time=time, uid=uid,
                   exit_status=exit_status, **kwargs)
        if reason:
            doc['reason'] = reason
        data = self.datafactory(data=doc,
                                 signature='insert_run_stop')
        try:
            self._post(self._rstop_url, data=data)
        except HTTPError:
            raise RuntimeError("Runstop already exits for {!r}".format(run_start))
        #self._cache_run_stop(doc,
        #                     self._RUNSTOP_CACHE)
        return uid

    def insert_descriptor(self, run_start, data_keys, time, uid, **kwargs):
        """Insert an EventDescriptor document in to database.

        Parameters
        ----------
        run_start : dict or str
            The RunStart to insert a Descriptor for.  Can be either
            a dict with a 'uid' key or a uid string
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
        kwargs = self._check_for_custom(kwargs)
        for k in data_keys:
            if '.' in k:
                raise ValueError("Key names cannot contain period '.':{}".format(k))
        data_keys = {k: dict(v) for k, v in data_keys.items()}
        run_start_uid = self.doc_or_uid_to_uid(run_start)
        descriptor = dict(run_start=run_start_uid, data_keys=data_keys,
                          time=time, uid=uid, **kwargs)
        data = self.datafactory(data=descriptor,
                                 signature='insert_descriptor')
        self._post(self._desc_url, data=data)
        self._cache_descriptor(descriptor=descriptor,
                               descriptor_cache=self._DESCRIPTOR_CACHE)
        return uid

    def insert_event(self, descriptor, time, seq_num, data, timestamps,
                     uid, validate=False, filled=None):
        """Create an event in metadatastore database backend

        .. warning

           This does not validate that the keys in `data` and `timestamps`
           match the data keys in `descriptor`.

        Parameters
        ----------
        descriptor : dict or str
            The Descriptor to insert event for.  Can be either
            a dict with a 'uid' key or a uid string
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
        filled : dict
            Dictionary of `False` or datum_ids. Keys are a subset of the keys
            in `data` and `timestamps` above.
        """
        if filled is None:
            filled = {}
        if validate:
            if data.keys() != timestamps.keys():
                raise ValueError(
                    BAD_KEYS_FMT.format(data.keys(),
                                        timestamps.keys()))
        descriptor_uid = self.doc_or_uid_to_uid(descriptor)
        data = dict(data)
        # Replace any filled data with the datum_id stashed in 'filled'.
        for k, v in six.iteritems(filled):
            if v:
                data[k] = v
        apply_to_dict_recursively(data, sanitize_np)
        timestamps = dict(timestamps)
        apply_to_dict_recursively(timestamps, sanitize_np)
        event = dict(descriptor=descriptor_uid, time=time, seq_num=seq_num, data=data,
                     timestamps=timestamps, uid=uid)
        data = self.datafactory(data=event, signature='insert_event')
        self._post(self._event_url, data=data)
        return uid

    def bulk_insert_events(self, descriptor, events, validate=False):
        """Bulk insert many events
        Parameters
        ----------
        event_descriptor : dict or str
            The Descriptor to insert event for.  Can be either
            a dict with a 'uid' key or a uid string
        events : iterable
        iterable of dicts matching the bs.Event schema
        validate : bool, optional
        If it should be checked that each pair of data/timestamps
        dicts has identical keys
        Returns
        -------
        ret : dict
            dictionary of details about the insertion
        """
        events = list(events) # if iterator, make json serializable
        def event_factory():
            for ev in events:
                # check keys, this could be expensive
                if validate:
                    if ev['data'].keys() != ev['timestamps'].keys():
                        raise ValueError(
                            BAD_KEYS_FMT.format(ev['data'].keys(),
                                                ev['timestamps'].keys()))
                data = dict(ev['data'])
                # Replace any filled data with the datum_id stashed in 'filled'.
                for k, v in six.iteritems(ev.get('filled', {})):
                    if v:
                        data[k] = v
                descriptor_uid = self.doc_or_uid_to_uid(descriptor)
                apply_to_dict_recursively(data, sanitize_np)
                timestamps = dict(ev['timestamps'])
                apply_to_dict_recursively(timestamps, sanitize_np)
                ev_out = dict(descriptor=descriptor_uid, uid=ev['uid'],
                              data=data, timestamps=timestamps,
                              time=ev['time'],
                              seq_num=ev['seq_num'])
                yield ev_out
        fixed_events = list(event_factory())
        payload = self.datafactory(data=dict(descriptor=descriptor,
                                             events=fixed_events,
                                             validate=validate),
                                   signature='bulk_insert_events')
        self._post(self._event_url, data=payload)
