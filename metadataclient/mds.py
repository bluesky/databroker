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

    def _cache_run_start(self, run_stop, run_stop_cache):
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
        r = request.post(url, json.dumps(data))
        r.raise_for_status()
        return r.json()

    def insert(self):
        pass

    def insert_run_start(self, time, uid, **kwargs):
        if 'custom' in kwargs:
            warnings.warn("Custom is a deprecated field")
            custom = kwargs.pop('custom')
            if any(k in kwargs for k in custom):
                raise TypeError("Duplicate keys in kwargs and custom")
            kwargs.update(custom)
        doc = dict(time=time, uid=uid, **kwargs)
        data = self.datafactory(data=doc, signature='insert_run_start')
        self._post(self._rstart_url, data=data)
        return uid

    def insert_run_stop():
        pass

    def insert_descriptor():
        pass

    def insert_event():
        pass

    def bulk_insert_events():
        pass
