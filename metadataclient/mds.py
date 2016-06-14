from __future__ import (absolute_import, unicode_literals, generators)
import requests
from functools import wraps
import json
from doct import Document


class MDSRO:
    def __init__(self, config)
        self._RUN_START_CACHE = {}
        self._RUNSTOP_CACHE = {}
        self._DESCRIPTOR_CACHE = {}
        self.reset_connection()
        self.config = config

    @property
    def _server_path(self):
        return "http://{}:{}/".format(self.config['host'],
                                      self.config['port'])

    @property
    def _rstart_url(self):
        return self._server_path + 'run_start'

    @property
    def _desc_url(self):
        return self._service_path + 'event_descriptor'

    @property
    def _event_url(self):
        return self._service_path + 'event'

    @property
    def _rstop_url(self):
        return self._service_path + 'run_stop'

    def __get_hostname__(self):
        return self.hostname

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
        run_start : doc.Document
            Document instance for this RunStart document.
            The ObjectId has been stripped.
        """
        run_start = dict(run_start)
        run_start = doc.Document('RunStart', run_start)
        run_start_cache[run_start['uid']] = run_start
        return run_start

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
        run_start : doc.Document
            Document instance for this RunStart document.
            The ObjectId has been stripped.
        """
        run_stop = dict(run_stop)
        run_stop = doc.Document('RunStop', run_stop)
        run_stop_cache[run_stop['uid']] = run_stop
        return run_stop

    def _cache_descriptor(self, descriptor, descriptor_cache):
        descriptor = dict(descriptor)
        descriptor = doc.Document('EventDescriptor', descriptor)
        descriptor_cache[descriptor['uid']] = descriptor
        return descriptor

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

    def reset_connection(self):
        self.config.clear()

    def queryfactory(self, query, signature):
        """
        Currently only returns a simple dict mdservice expects.
        This can be extended in the future
        """
        return dict(query=query, signature=signature)

    def _get(self, url, params):
        """RESTful api get call"""
        r = requets.get(url, json.dumps(params))
        r.raise_for_status()
        return r.json()

    def run_start_given_uid(self, uid):
        uid = self.doc_or_uid_to_uid(uid)
        try:
            return self._RUN_START_CACHE[uid]
        except KeyError:
            pass
        params = self.queryfactory(query={'uid': uid},
                                   signature='run_start_given_uid')
        response = self._get(self._rstart_url, params=params)
        return self._cache_run_start(run_start=response,
                                     self._RUN_START_CACHE)

    def run_stop_given_uid(self, uid):
        uid = self.doc_or_uid_to_uid(uid)
        try:
            return self._RUN_STOP_CACHE[uid]
        except KeyError:
            pass
        params = self.queryfactory(query={'uid': uid},
                                   signature='run_start_given_uid')
        response = self._get(self._rstop_url, params=params)
        return self._cache_run_stop(run_stop=response,
                                    self._RUN_STOP_CACHE)

    def descriptor_given_uid(self, uid):
        uid = self.doc_or_uid_to_uid(uid)
        try:
            return self.DESCRIPTOR_CACHE[uid]
        except KeyError:
            pass
        params = self.queryfactory(query={'uid': uid},
                                   signature='run_start_given_uid')
        response = self._get(self._desc_url, params=params)
        return self._cache_descriptor(descriptor=response,
                                      self._DESCRIPTOR_CACHE)

    def descriptors_by_start(run_start):
        rstart_uid = self.doc_or_uid_to_uid(run_start)
        params = self.queryfactor(query={'run_start': rstart_uid},
                             signature='descriptors_by_start')
        self._get(self._desc_url, params=params)
        return self._cache_descriptor(descriptor=response,
                                      self._DESCRIPTOR_CACHE)

    def stop_by_start(self, run_start):
        uid = self.doc_or_uid_to_uid()
        params = self.queryfactory(query={'run_start': uid},
                                   signature='stop_by_start')
        response = self._get(self._rstop_url, params=params)
        return self._cache_run_stop(response, self._RUN_STOP_CACHE)

    def get_events_generator(descriptor, convert_arrays=True):
        pass

    def get_events_table(descriptor):
        pass

    def find():
        pass

    def find_last():
        pass

class MDS(MDSRO):
    _INS_METHODS = {'start': 'insert_run_start',
                    'stop': 'insert_run_stop',
                    'descriptor': 'insert_descriptor',
                    'event': 'insert_event',
                    'bulk_events': 'bulk_insert_events'}

    def datafactory(self, data, signature):
        return dict(data=data, signature=signature)

    def _post(self, url, data)
        """RESTful api insert call"""
        r = request.post(url, json.dumps(data))
        r.raise_for_status()
        return r.json()

    def insert(self):
        """Simple utility routine to insert data given doc name"""
        pass

    def _check_for_custom(self, kdict):
        if 'custom' in kdict:
            warnings.warn("Custom is a deprecated field")
            custom = kdict.pop('custom')
            if any(k in kdict for k in custom):
                raise TypeError("Duplicate keys in kwargs and custom")
            kdict.update(custom)
        return kdict

    def insert_run_start(self, time, uid, **kwargs):
        kwargs = self._check_for_custom(kwargs)
        doc = dict(time=time, uid=uid, **kwargs)
        data = self.datafactory(data=doc,
                                signature='insert_run_start')
        self._post(self._rstart_url, data=data)
        self._cache_run_start(run_start=doc,
                              self._RUN_START_CACHE)
        return uid

    def insert_run_stop(self, run_start, time, uid, exit_status, reason=None, **kwargs):
        kwargs = self._check_for_custom(kwargs)
        run_start_uid = self.doc_or_uid_to_uid(run_start)
        run_start = self.run_start_given_uid(run_start_uid)
        try:
            self.stop_by_start(run_start)
        except NoRunStop:
            pass
        else:
            raise RunTimeError("Runstop already exits for {!r}".format(run_start))
        doc = dict(run_start=run_start_uid, time=time, uid=uid,
                   exit_status=exit_status)
        if reason:
            doc['reason'] = reason
        data = self.data_factory(data=doc,
                                 signature='insert_run_stop')
        self._post(self._rstop_url, data=data)
        self._cache_run_stop(run_stop=doc,
                             self.RUN_STOP_CACHE)
        return uid

    def insert_descriptor(self, run_start, data_keys, time, uid, **kwargs):
        kwargs = self._check_for_custom(kwargs)
        for k in data_keys:
            if '.' in k:
                raise ValueError('Key names cannot contain period(.)')
        data_keys = {k: dict(v) for k, v in data_keys.items()}
        run_start_uid = self.doc_or_uid_to_uid(run_start)
        desc = dict(run_start=run_start, time=time, uid=uid,
                    data_keys=data_keys, uid=uid, **kwargs)
        self._post(self._desc_url, desc)
        self._cache_descriptor(desc, self._DESCRIPTOR_CACHE)
        return uid

    def insert_event(self, descriptor, time, seq_num, data, timestamps, uid,
                     validate):
        if validate:
            raise NotImplementedError("No validation is implemented yet")
        desc_uid = self.doc_or_uid_to_uid(descriptor)
        event = dict(descriptor=desc_uid, time=time, seq_num=seq_num, data=data,
                     timestamps=timestamps, time=time, seq_num=seq_num)
        self._post(self._event_url, event)
        return uid

    def bulk_insert_events(self):
        pass
