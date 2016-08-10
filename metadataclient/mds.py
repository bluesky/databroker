from __future__ import (absolute_import, unicode_literals, generators)
import requests
from functools import wraps
import json
from doct import Document
import six
import warnings
import logging
from requests import HTTPError
from metadatastore.core import (NoRunStart, NoEventDescriptors, NoRunStop,
                               BAD_KEYS_FMT)

logger = logging.getLogger(__name__)


class MDSRO:
    def __init__(self, config):
        self._RUN_START_CACHE = {}
        self._RUNSTOP_CACHE = {}
        self._DESCRIPTOR_CACHE = {}
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
        return self._server_path + 'event_descriptor'

    @property
    def _event_url(self):
        return self._server_path + 'event'

    @property
    def _rstop_url(self):
        return self._server_path + 'run_stop'

    def __get_hostname__(self):
        return self.hostname

    def cache_document(self, doc, doc_type, doc_cache):
        doc = dict(doc)
        doc = Document(doc_type, doc)
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
        run_start : doc.Document
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
        run_start : doc.Document
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

    def reset_connection(self):
        self.config.clear()

    def queryfactory(self, query, signature):
        """
        Currently only returns a simple dict mdservice expects.
        This can be extended in the future
        """
        return dict(query=query, signature=signature)

    def _get(self, url, params):
        r = requests.get(url, json.dumps(params))
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
        if not response:
            raise NoRunStart('No RunStart found with uid {}'.format(uid))
        return response

    def find_run_starts(self, **kwargs):
        params = self.queryfactory(query=kwargs,
                                   signature='find_run_starts')
        response = self._get(self._rstart_url, params=params)
        for r in response:
            yield Document('RunStart', r)

    def find_descriptors(self, **kwargs):
        params = self.queryfactory(query=kwargs,
                                   signature='find_descriptors')
        response = self._get(self._desc_url, params=params)
        for r in response:
            r['run_start'] = Document('RunStart', r['run_start'])
            yield Document('EventDescriptor', r)


    def find_run_stops(self, **kwargs):
        params = self.queryfactory(query=kwargs,
                                   signature='find_run_stops')
        response = self._get(self._rstop_url, params=params)
        for r in response:
            r['run_start'] = Document('RunStart', r['run_start'])
            yield Document('RunStop', r)

    def run_stop_given_uid(self, uid):
        uid = self.doc_or_uid_to_uid(uid)
        try:
            return self._RUNSTOP_CACHE[uid]
        except KeyError:
            pass
        params = self.queryfactory(query={'uid': uid},
                                   signature='run_stop_given_uid')
        response = self._get(self._rstop_url, params=params)
        response['run_start'] = Document('RunStart', response['run_start'])
        response = Document('RunStop', response)
        return response

    def descriptor_given_uid(self, uid):
        uid = self.doc_or_uid_to_uid(uid)
        params = self.queryfactory(query={'uid': uid},
                                   signature='descriptor_given_uid')
        response = self._get(self._desc_url, params=params)
        return response
        #return self._cache_descriptor(response,
        #                              self._DESCRIPTOR_CACHE)

    def descriptors_by_start(self, run_start):
        rstart_uid = self.doc_or_uid_to_uid(run_start)
        params = self.queryfactory(query={'run_start': rstart_uid},
                                   signature='descriptors_by_start')
        response = self._get(self._desc_url, params=params)
        if not response:
            raise NoEventDescriptors('No descriptor is found provided run_start {}'.format(rstart_uid))
        return response
        #return self._cache_descriptor(response,
        #                              self._DESCRIPTOR_CACHE)

    def stop_by_start(self, run_start):
        uid = self.doc_or_uid_to_uid(run_start)
        params = self.queryfactory(query={'run_start': uid},
                                   signature='stop_by_start')
        response = self._get(self._rstop_url, params=params)
        response['run_start'] = Document('RunStart', response['run_start'])
        return Document('RunStop', response)
        # return self._cache_run_stop(response, self._RUNSTOP_CACHE)

    def get_events_generator(self, descriptor, convert_arrays=True):
        descriptor_uid = self.doc_or_uid_to_uid(descriptor)
        descriptor = self.descriptor_given_uid(descriptor_uid)
        params = self.queryfactory(query={'descriptor': descriptor,
                                          'convert_arrays': convert_arrays},
                                   signature='get_events_generator')
        events = self._get(self._event_url, params=params)
        for e in events:
            e['descriptor'] = descriptor
            yield e

    def get_events_table(self, descriptor):
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
        run_start doc.Document
        The requested RunStart documents
        """
        params = self.queryfactory(query={'num': num},
                                   signature='find_last')
        response = self._get(self._rstart_url, params=params)
        for r in response:
            yield Document('RunStart', r)


class MDS(MDSRO):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self._INS_METHODS = {'start': 'insert_run_start',
                             'stop': 'insert_run_stop',
                             'descriptor': 'insert_descriptor',
                             'event': 'insert_event',
                             'bulk_events': 'bulk_insert_events'}

    def datafactory(self, data, signature):
        return dict(data=data, signature=signature)

    def _post(self, url, data):
        r = requests.post(url, json.dumps(data))
        r.raise_for_status()
        return r.json()

    def insert(self, name, doc):
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
                     uid, validate=False):
        if validate:
            raise NotImplementedError('Insert event validation not written yet')
        descriptor_uid = self.doc_or_uid_to_uid(descriptor)
        event = dict(descriptor=descriptor_uid, time=time, seq_num=seq_num, data=data,
                     timestamps=timestamps, uid=uid)
        data = self.datafactory(data=event, signature='insert_event')
        self._post(self._event_url, data=data)
        return uid

    def bulk_insert_events(self, descriptor, events, validate):
        events = list(events)
        def event_factory():
            for ev in events:
                # check keys, this could be expensive
                if validate:
                    if ev['data'].keys() != ev['timestamps'].keys():
                        raise ValueError(
                            BAD_KEYS_FMT.format(ev['data'].keys(),
                                                ev['timestamps'].keys()))
                descriptor_uid = self.doc_or_uid_to_uid(descriptor)
                ev_out = dict(descriptor=descriptor_uid, uid=ev['uid'],
                            data=ev['data'], timestamps=ev['timestamps'],
                            time=ev['time'],
                            seq_num=ev['seq_num'])
                yield ev_out
        d = list(event_factory())
        payload = self.datafactory(data=dict(descriptor=descriptor, events=events,
                                   validate=validate), signature='bulk_insert_events')
        self._post(self._event_url, data=payload)
