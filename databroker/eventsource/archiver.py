# This module is experimental. It is not documented or covered by automated
# tests. It may change in a backward-incompatible way in a future release of
# bluesky.
from __future__ import (absolute_import, division, print_function,
                        unicode_literals)
import six

from functools import partial
from datetime import datetime
import pytz
import uuid

import requests
import pandas as pd


class ArchiverEventSource(object):
    def __init__(self, url, timezone, pvs):
        """
        DataBroker plugin

        Parameters
        ----------
        url : string
            e.g., 'http://host:port/'
        timezone : string
            e.g., 'US/Eastern'
        pvs : dict
            a dict mapping human-friendly names to PVs
        """
        if not url.endswith('/'):
            url += '/'
        self.url = url
        self.archiver_addr = self.url + "retrieval/data/getData.json"
        self.tz = pytz.timezone(timezone)
        self.pvs = pvs
        self._descriptors = {}

    def insert(self, name, doc):
        raise NotImplementedError()

    def stream_names_given_header(self, header):
        # We actually don't use the header in this case.
        return ['archiver_{}'.format(name) for name in self.pvs]

    def fields_given_header(self, header):
        # We actually don't use the header in this case.
        return list(self.pvs)

    def descriptors_given_header(self, header):
        run_start_uid = header['start']['uid']
        try:
            return self._descriptors[run_start_uid]
        except KeyError:
            # Mock up descriptors and cache them so that the ephemeral uid is
            # stable for the duration of this process.
            descs = []
            since = header['start']['time'],
            until = header['stop']['time']
            for name, pv in six.iteritems(self.pvs):
                data_keys = {name: {'source': pv,
                                    'dtype': 'number',
                                    'shape': []}}
                _from = _munge_time(since[0], self.tz)
                # because since is a tuple^
                _to = _munge_time(until, self.tz)
                params = {'pv': pv, 'from': _from, 'to': _to}
                desc = {'time': header['start']['time'],
                        'uid': 'empheral-' + str(uuid.uuid4()),
                        'data_keys': data_keys,
                        'run_start': header['start']['uid'],
                        'external_query': params,
                        'external_url': self.url}
                descs.append(desc)
            self._descriptors[run_start_uid] = descs
            return [ d for d in self._descriptors]
            #prepare = partial(self.prepare_hook, 'descriptor')
            #return list(map(prepare, self._descriptors[run_start_uid]))

    def docs_given_header(self, header,fill=False, fields=None,
                          **kwargs):
        desc_uids = {}
        for d in self.descriptors_given_header(header):
            # Stash the desc uids in a local var so we can use them in events.
            pv = list(d['data_keys'].values())[0]['source']
            desc_uids[pv] = d['uid']
            yield d
        since, until = header['start']['time'], header['stop']['time']
        for name, pv in six.iteritems(self.pvs):
            _from = _munge_time(since, self.tz)
            _to = _munge_time(until, self.tz)
            params = {'pv': pv, 'from': _from, 'to': _to}
            req = requests.get(self.archiver_addr, params=params, stream=True)
            req.raise_for_status()
            raw, = req.json()
            timestamps = [x['secs'] for x in raw['data']]
            data = [x['val'] for x in raw['data']]
            for seq_num, (d, t) in enumerate(zip(data, timestamps)):
                doc = {'data': {name: d},
                       'timestamps': {name: t},
                       'time': t,
                       'uid': 'ephemeral-' + str(uuid.uuid4()),
                       'seq_num': seq_num,
                       'descriptor': desc_uids[pv]}
                yield doc               

    def table_given_header(self, header, fields=None):
        # TODO: Add timezone goodies
        no_fields_filter = False
        if fields is None:
            no_fields_filter = True
            fields = []
        fields = set(fields)
        docs = list(self.docs_given_header(header=header,
                                    fill=False, fields=fields))
        return pd.DataFrame.from_records(data=docs, index='seq_num')

    def fill_event(self, *args, **kwrags):
        raise NotImplementedError()

    def fill_table(self, *args, **kwargs):
        raise NotImplementedError()


def _munge_time(t, timezone):
    """Close your eyes and trust @arkilic

    Parameters
    ----------
    t : float
        POSIX (seconds since 1970)
    timezone : pytz object
        e.g. ``pytz.timezone('US/Eastern')``

    Return
    ------
    time
        as ISO-8601 format
    """
    t = datetime.fromtimestamp(t)
    return timezone.localize(t).replace(microsecond=0).isoformat()
